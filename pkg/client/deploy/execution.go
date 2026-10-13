package deploy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/psviderski/uncloud/internal/secret"
	"github.com/psviderski/uncloud/pkg/api"
	"github.com/psviderski/uncloud/pkg/client/deploy/operation"
	"github.com/psviderski/uncloud/pkg/distlock"
)

// PlanStatus is the terminal or in-progress state of a persisted deployment plan.
type PlanStatus string

const (
	// planStatusRunning means the plan has been created but hasn't finished yet. A client rerun resumes it.
	planStatusRunning PlanStatus = "running"
	// planStatusCompleted means every step of the plan has been confirmed against Docker and the store.
	planStatusCompleted PlanStatus = "completed"
	// planStatusFailed means a step reached a definitive failure (e.g. a container never became healthy or a hook
	// exited with an error). A rerun creates a new plan instead of retrying the failed one.
	planStatusFailed PlanStatus = "failed"
	// planStatusSuperseded means a newer deployment, scaling, stop, or removal fenced the service generation while
	// the plan was in progress. The plan must not perform any more mutations.
	planStatusSuperseded PlanStatus = "superseded"
)

// StepStatus is the execution state of a single persisted plan step.
type StepStatus string

const (
	stepPending StepStatus = "pending"
	stepRunning StepStatus = "running"
	stepDone    StepStatus = "done"
	stepFailed  StepStatus = "failed"
)

// StepKind identifies the kind of a persisted plan step. The values mirror the in-memory deployment operations
// but are stable across client versions, so they are stored explicitly instead of relying on Go type names.
type StepKind string

const (
	stepRunContainer     StepKind = "run_container"
	stepReplaceContainer StepKind = "replace_container"
	stepStopContainer    StepKind = "stop_container"
	stepRemoveContainer  StepKind = "remove_container"
	stepStopPreDeploy    StepKind = "stop_predeploy"
	stepRunPreDeploy     StepKind = "run_predeploy"
)

// Replace step sub-phases, persisted in the same order as the original ReplaceContainerOperation executes.
const (
	// phaseCreated means the new container has been created (ID persisted in NewContainerID).
	phaseCreated = "created"
	// phaseOldStopped means the old container has been stopped (before starting the new one for stop-first,
	// or after the new one became healthy for start-first).
	phaseOldStopped = "old_stopped"
	// phaseStarted means the new container has been started.
	phaseStarted = "started"
	// phaseHealthy means the new container health has been confirmed. The old container can now be removed.
	phaseHealthy = "healthy"
)

// ErrPlanSuperseded is returned when a deployment plan is resumed or executed after a newer deployment, scaling,
// service stop, or removal advanced the service generation. The plan performs no further mutations.
var ErrPlanSuperseded = errors.New("deployment plan superseded by a newer deployment, scaling, service stop or removal")

// errContainerOwnedByNewerDeployment indicates a step is about to mutate a container created by a newer
// deployment generation. It is treated as plan supersession.
var errContainerOwnedByNewerDeployment = errors.New("container belongs to a newer deployment generation")

// PlanStore persists deployment plans and service generations in the distributed cluster store.
// The concrete implementation is the cluster client. Test doubles may provide in-memory implementations.
type PlanStore interface {
	GetDeploymentPlan(ctx context.Context, serviceID, planID string) (api.DeploymentPlanRecord, error)
	PutDeploymentPlan(ctx context.Context, serviceID, planID string, data []byte) error
	ListDeploymentPlans(ctx context.Context, serviceID string) ([]api.DeploymentPlanRecord, error)
	DeleteDeploymentPlan(ctx context.Context, serviceID, planID string) error
	GetServiceDeploymentGeneration(ctx context.Context, serviceID string) (uint64, error)
	AdvanceServiceDeploymentGeneration(ctx context.Context, serviceID string) (uint64, error)
}

// ExecutionClient is the cluster client surface required to durably execute deployment plans.
type ExecutionClient interface {
	operation.Client
	PlanStore
}

// deploymentLease is the lease handle the executor uses. *distlock.Lease satisfies it.
type deploymentLease interface {
	Context() context.Context
	Release(ctx context.Context) error
}

// deploymentLockerFactory creates distributed lockers. The concrete cluster client provides it; test doubles
// may omit it, in which case the executor runs without a distributed lease (single-process tests only).
type deploymentLockerFactory interface {
	NewLocker(config distlock.Config) (*distlock.Locker, error)
}

// PersistedPlan is the durable state of one parsed service deployment. It fixes the participating machines,
// volume constraints, existing containers, and update order resolved at planning time, and records the execution
// state of every step. It is serialized to the cluster store after every state transition.
type PersistedPlan struct {
	// ID is the stable deployment plan ID.
	ID string `json:"id"`
	// ServiceID and ServiceName identify the service this plan reconciles.
	ServiceID   string `json:"service_id"`
	ServiceName string `json:"service_name"`
	// Generation is the service deployment generation (fencing token) allocated when the plan was created. The
	// plan may mutate the service only while the current service generation equals this value.
	Generation uint64 `json:"generation"`
	// Spec is the resolved desired service spec captured by the plan. Environment variables are stripped before
	// persistence to avoid leaking secrets through the replicated store; a resuming client supplies them from a
	// freshly resolved spec with the same identity.
	Spec api.ServiceSpec `json:"spec"`
	// SkipHealthMonitor mirrors the strategy option captured by every health-checking step.
	SkipHealthMonitor bool `json:"skip_health_monitor,omitempty"`
	// Status is the current plan status.
	Status PlanStatus `json:"status"`
	// Steps is the ordered list of persisted steps, fixed at plan creation.
	Steps []*PersistedStep `json:"steps"`
	// Error records the last failure message when Status is planStatusFailed.
	Error string `json:"error,omitempty"`
	// CreatedAt and UpdatedAt track the plan lifecycle.
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// PersistedStep is the durable state of one plan step.
type PersistedStep struct {
	// Kind is the stable step kind.
	Kind StepKind `json:"kind"`
	// Status is the current step status.
	Status StepStatus `json:"status"`
	// Error records a failure message when Status is stepFailed.
	Error string `json:"error,omitempty"`

	// MachineID and MachineName fix the machine that participates in this step.
	MachineID   string `json:"machine_id"`
	MachineName string `json:"machine_name"`

	// Spec is the desired service spec for steps that create a container. Env is stripped before persistence.
	Spec api.ServiceSpec `json:"spec"`
	// Order is the update order ("start-first"/"stop-first") for replace steps.
	Order string `json:"order,omitempty"`
	// SkipHealthMonitor disables health monitoring for steps that create a container.
	SkipHealthMonitor bool `json:"skip_health_monitor,omitempty"`
	// StopGracePeriod is the stop timeout for steps that stop a container.
	StopGracePeriod *time.Duration `json:"stop_grace_period,omitempty"`

	// ContainerID is the existing container targeted by stop/remove/replace/stop-predeploy steps.
	ContainerID string `json:"container_id,omitempty"`
	// Container is the snapshot of the existing container captured at planning time. Env is stripped.
	Container api.ServiceContainer `json:"container,omitempty"`
	// OldHookContainerIDs are previous pre-deploy hook containers to remove before running a new hook.
	OldHookContainerIDs []string `json:"old_hook_ids,omitempty"`

	// NewContainerID and NewContainerName identify the container created by a run/replace/run-predeploy step.
	// They are persisted immediately after the creation response so a restart can adopt the container even when
	// later phases were never reached.
	NewContainerID   string `json:"new_container_id,omitempty"`
	NewContainerName string `json:"new_container_name,omitempty"`
	// Phase is the step-specific sub-phase (one of the phase* constants).
	Phase string `json:"phase,omitempty"`
	// WasRunning records whether the old container was running before a stop-first replacement stopped it, so a
	// health failure can roll back by restarting it.
	WasRunning bool `json:"was_running,omitempty"`
	// OldStopped records that the old container has already been stopped.
	OldStopped bool `json:"old_stopped,omitempty"`
	// RolledBack records that the old container was restarted after a failed replacement.
	RolledBack bool `json:"rolled_back,omitempty"`
	// RemovedHookIDs tracks old hook containers already removed by a run-predeploy step.
	RemovedHookIDs []string `json:"removed_hook_ids,omitempty"`
}

// persistedStepWire is the JSON representation of a step with the container snapshot kept as raw JSON. Steps
// without a captured container omit it entirely: an empty api.ServiceContainer JSON would otherwise fail the
// mandatory-fields validation of api.Container.UnmarshalJSON on decode.
type persistedStepWire struct {
	Kind                StepKind        `json:"kind"`
	Status              StepStatus      `json:"status"`
	Error               string          `json:"error,omitempty"`
	MachineID           string          `json:"machine_id"`
	MachineName         string          `json:"machine_name"`
	Spec                api.ServiceSpec `json:"spec"`
	Order               string          `json:"order,omitempty"`
	SkipHealthMonitor   bool            `json:"skip_health_monitor,omitempty"`
	StopGracePeriod     *time.Duration  `json:"stop_grace_period,omitempty"`
	ContainerID         string          `json:"container_id,omitempty"`
	Container           json.RawMessage `json:"container,omitempty"`
	OldHookContainerIDs []string        `json:"old_hook_ids,omitempty"`
	NewContainerID      string          `json:"new_container_id,omitempty"`
	NewContainerName    string          `json:"new_container_name,omitempty"`
	Phase               string          `json:"phase,omitempty"`
	WasRunning          bool            `json:"was_running,omitempty"`
	OldStopped          bool            `json:"old_stopped,omitempty"`
	RolledBack          bool            `json:"rolled_back,omitempty"`
	RemovedHookIDs      []string        `json:"removed_hook_ids,omitempty"`
}

// MarshalJSON serializes the step, including the existing-container snapshot only when one was captured.
func (s PersistedStep) MarshalJSON() ([]byte, error) {
	wire := persistedStepWire{
		Kind:                s.Kind,
		Status:              s.Status,
		Error:               s.Error,
		MachineID:           s.MachineID,
		MachineName:         s.MachineName,
		Spec:                s.Spec,
		Order:               s.Order,
		SkipHealthMonitor:   s.SkipHealthMonitor,
		StopGracePeriod:     s.StopGracePeriod,
		ContainerID:         s.ContainerID,
		OldHookContainerIDs: s.OldHookContainerIDs,
		NewContainerID:      s.NewContainerID,
		NewContainerName:    s.NewContainerName,
		Phase:               s.Phase,
		WasRunning:          s.WasRunning,
		OldStopped:          s.OldStopped,
		RolledBack:          s.RolledBack,
		RemovedHookIDs:      s.RemovedHookIDs,
	}
	if s.Container.ContainerJSONBase != nil {
		containerJSON, err := json.Marshal(s.Container)
		if err != nil {
			return nil, err
		}
		wire.Container = containerJSON
	}
	return json.Marshal(wire)
}

// UnmarshalJSON decodes the step, restoring the container snapshot only when one was persisted.
func (s *PersistedStep) UnmarshalJSON(data []byte) error {
	var wire persistedStepWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*s = PersistedStep{
		Kind:                wire.Kind,
		Status:              wire.Status,
		Error:               wire.Error,
		MachineID:           wire.MachineID,
		MachineName:         wire.MachineName,
		Spec:                wire.Spec,
		Order:               wire.Order,
		SkipHealthMonitor:   wire.SkipHealthMonitor,
		StopGracePeriod:     wire.StopGracePeriod,
		ContainerID:         wire.ContainerID,
		OldHookContainerIDs: wire.OldHookContainerIDs,
		NewContainerID:      wire.NewContainerID,
		NewContainerName:    wire.NewContainerName,
		Phase:               wire.Phase,
		WasRunning:          wire.WasRunning,
		OldStopped:          wire.OldStopped,
		RolledBack:          wire.RolledBack,
		RemovedHookIDs:      wire.RemovedHookIDs,
	}
	if len(wire.Container) > 0 && string(wire.Container) != "null" {
		if err := json.Unmarshal(wire.Container, &s.Container); err != nil {
			return fmt.Errorf("unmarshal persisted step container: %w", err)
		}
	}
	return nil
}

// planExecutor drives a ServicePlan through its durable persisted plan: it resumes an interrupted plan or
// creates a new one, persists every step transition, reconciles each step against the actual Docker state, and
// fences itself against newer deployments using the service generation.
type planExecutor struct {
	client ExecutionClient
	// servicePlan is the freshly planned service deployment for this client invocation.
	servicePlan *ServicePlan
}

func newPlanExecutor(cli ExecutionClient, sp *ServicePlan) *planExecutor {
	return &planExecutor{client: cli, servicePlan: sp}
}

// Execute runs the service plan durably: it acquires the per-service deployment lease when available, resumes an
// interrupted plan with the same identity, or allocates a new fenced generation and creates one, then drives all
// steps to completion, failure, or supersession.
func (e *planExecutor) Execute(ctx context.Context) error {
	sp := e.servicePlan
	if sp == nil {
		return errors.New("service plan is nil")
	}

	// Acquire the distributed per-service lease so two clients can't plan or fence the same service generation
	// concurrently. The lease is auto-renewed; losing it cancels the execution context.
	var lease deploymentLease
	if lockerFactory, ok := e.client.(deploymentLockerFactory); ok {
		acquired, err := e.acquireLease(ctx, lockerFactory)
		if err != nil {
			return err
		}
		lease = acquired

		var cancel context.CancelCauseFunc
		ctx, cancel = context.WithCancelCause(ctx)
		go func() {
			select {
			case <-lease.Context().Done():
				cancel(context.Cause(lease.Context()))
			case <-ctx.Done():
			}
		}()
		defer cancel(nil)
		defer func() { _ = lease.Release(context.WithoutCancel(ctx)) }()
	}

	plan, err := e.resumeOrCreatePlan(ctx)
	if err != nil {
		return err
	}
	sp.persisted = plan

	if len(plan.Steps) == 0 {
		// A no-op deployment persists nothing. An interrupted plan whose steps are all done is reconciled to
		// completed below; absent such a plan there is nothing to execute.
		return nil
	}

	// liveSpec is the freshly resolved spec including secrets. It is used only in-memory to create containers;
	// the persisted plan keeps an env-stripped copy. When resuming, resumeOrCreatePlan verified it has the same
	// identity as the stored spec.
	liveSpec := sp.Spec

	runErr := e.driveSteps(ctx, plan, liveSpec)
	switch {
	case runErr == nil:
		plan.Status = planStatusCompleted
		plan.Error = ""
		if err = e.save(ctx, plan); err != nil {
			return fmt.Errorf("mark deployment plan %s completed: %w", plan.ID, err)
		}
		return nil
	case errors.Is(runErr, ErrPlanSuperseded):
		plan.Status = planStatusSuperseded
		plan.Error = runErr.Error()
		_ = e.save(context.WithoutCancel(ctx), plan)
		return runErr
	case isDefinitiveFailure(runErr):
		plan.Status = planStatusFailed
		plan.Error = runErr.Error()
		_ = e.save(context.WithoutCancel(ctx), plan)
		return runErr
	default:
		// Transient errors (unreachable machine, lost response, cancelled context, lost lease) leave the plan
		// running so the next client invocation resumes it from the persisted state.
		plan.Error = runErr.Error()
		_ = e.save(context.WithoutCancel(ctx), plan)
		return runErr
	}
}

func (e *planExecutor) acquireLease(
	ctx context.Context, factory deploymentLockerFactory,
) (deploymentLease, error) {
	locker, err := factory.NewLocker(distlock.Config{})
	if err != nil {
		return nil, fmt.Errorf("create deployment locker: %w", err)
	}
	lease, err := locker.Acquire(ctx, DeploymentLeaseResource(e.servicePlan.ServiceID))
	if err != nil {
		return nil, fmt.Errorf("acquire deployment lease for service %s: %w", e.servicePlan.ServiceName, err)
	}
	return lease, nil
}

// resumeOrCreatePlan returns the plan to execute: an interrupted running plan with the same desired spec is
// resumed so the fixed machines, containers, volume constraints, and update order are preserved. A running plan
// with a different spec is superseded by the new one.
func (e *planExecutor) resumeOrCreatePlan(ctx context.Context) (*PersistedPlan, error) {
	sp := e.servicePlan

	records, err := e.client.ListDeploymentPlans(ctx, sp.ServiceID)
	if err != nil {
		return nil, fmt.Errorf("list deployment plans for service %s: %w", sp.ServiceName, err)
	}

	// A running plan whose generation is no longer current was already fenced by a newer deployment, scaling,
	// stop, or removal. Supersede it before planning so the current invocation creates the plan it just asked for
	// instead of returning a supersession error and requiring another run.
	currentGeneration, err := e.client.GetServiceDeploymentGeneration(ctx, sp.ServiceID)
	if err != nil {
		return nil, fmt.Errorf("get service deployment generation: %w", err)
	}

	// Only one running plan is expected at a time (the lease serializes plan creation). If several exist, take
	// the one with the highest generation as the most recent.
	var running *PersistedPlan
	for _, record := range records {
		plan, decodeErr := decodePlan(record.Data)
		if decodeErr != nil {
			slog.Warn("Skipping unreadable deployment plan record.",
				"service", sp.ServiceName, "plan", record.ID, "err", decodeErr)
			continue
		}
		plan.ID = record.ID
		if plan.Status != planStatusRunning {
			continue
		}
		if plan.Generation != currentGeneration {
			// Fenced by a newer operation: mark superseded and never resume it.
			plan.Status = planStatusSuperseded
			plan.Error = ErrPlanSuperseded.Error()
			if saveErr := e.save(ctx, plan); saveErr != nil {
				return nil, fmt.Errorf("supersede deployment plan %s: %w", plan.ID, saveErr)
			}
			continue
		}
		if running == nil || plan.Generation > running.Generation {
			running = plan
		}
	}

	if running != nil {
		if specsEqualForResume(running.Spec, sp.Spec) {
			// Same parsed service spec: resume the interrupted plan instead of replanning, so no extra container
			// is created and the pre-deploy hook is not executed twice.
			return running, nil
		}
		// The desired spec changed: the new deployment supersedes the interrupted plan.
		running.Status = planStatusSuperseded
		running.Error = ErrPlanSuperseded.Error()
		if err = e.save(ctx, running); err != nil {
			return nil, fmt.Errorf("supersede deployment plan %s: %w", running.ID, err)
		}
	}

	return e.createPlan(ctx, sp)
}

// createPlan allocates the next fenced generation, builds a persisted plan from the freshly planned operations,
// and persists it in running status before any mutation can happen.
func (e *planExecutor) createPlan(ctx context.Context, sp *ServicePlan) (*PersistedPlan, error) {
	if len(sp.Operations) == 0 {
		// Nothing to do: don't allocate a generation or persist a plan. This keeps no-change redeployments
		// exactly as they were before durable plans existed.
		return &PersistedPlan{
			ServiceID:   sp.ServiceID,
			ServiceName: sp.ServiceName,
			Spec:        sp.Spec,
			Status:      planStatusRunning,
		}, nil
	}

	generation, err := e.client.AdvanceServiceDeploymentGeneration(ctx, sp.ServiceID)
	if err != nil {
		return nil, fmt.Errorf("allocate deployment generation for service %s: %w", sp.ServiceName, err)
	}

	id, err := secret.NewID()
	if err != nil {
		return nil, fmt.Errorf("generate deployment plan ID: %w", err)
	}

	now := time.Now().UTC()
	plan := &PersistedPlan{
		ID:                id,
		ServiceID:         sp.ServiceID,
		ServiceName:       sp.ServiceName,
		Generation:        generation,
		Spec:              sp.Spec,
		SkipHealthMonitor: sp.skipHealthMonitor(),
		Status:            planStatusRunning,
		Steps:             buildPersistedSteps(sp, generation),
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	if err = e.save(ctx, plan); err != nil {
		return nil, fmt.Errorf("persist deployment plan %s: %w", id, err)
	}
	return plan, nil
}

// skipHealthMonitor reports whether any planned step skips health monitoring (strategy option).
func (sp *ServicePlan) skipHealthMonitor() bool {
	for _, op := range sp.Operations {
		switch o := op.(type) {
		case *operation.RunContainerOperation:
			if o.SkipHealthMonitor {
				return true
			}
		case *operation.ReplaceContainerOperation:
			if o.SkipHealthMonitor {
				return true
			}
		}
	}
	return false
}

// checkFence verifies the plan is still the current deployment of its service. Any newer deployment, scaling,
// stop, or removal has advanced the generation, in which case ErrPlanSuperseded is returned.
func (e *planExecutor) checkFence(ctx context.Context, plan *PersistedPlan) error {
	generation, err := e.client.GetServiceDeploymentGeneration(ctx, plan.ServiceID)
	if err != nil {
		return fmt.Errorf("get service deployment generation: %w", err)
	}
	if generation != plan.Generation {
		return fmt.Errorf("%w: plan generation %d, current generation %d",
			ErrPlanSuperseded, plan.Generation, generation)
	}
	return nil
}

// ensurePlanOwnsContainer verifies an existing container the plan is about to stop or remove was not created by a
// newer deployment generation. Legacy containers without a generation label are treated as generation 0.
func ensurePlanOwnsContainer(ctr api.ServiceContainer, plan *PersistedPlan) error {
	if generation := ctr.DeploymentGeneration(); generation > plan.Generation {
		return fmt.Errorf("%w: container %s is from generation %d, plan generation %d",
			errContainerOwnedByNewerDeployment, ctr.ShortID(), generation, plan.Generation)
	}
	return nil
}

// save persists the plan with environment variables stripped and a fresh update timestamp.
func (e *planExecutor) save(ctx context.Context, plan *PersistedPlan) error {
	plan.UpdatedAt = time.Now().UTC()
	data, err := encodePlan(plan)
	if err != nil {
		return err
	}
	return e.client.PutDeploymentPlan(ctx, plan.ServiceID, plan.ID, data)
}

// encodePlan serializes the plan for persistence, stripping environment variables (including secrets) from specs
// and container snapshots so the replicated cluster store never receives them.
func encodePlan(plan *PersistedPlan) ([]byte, error) {
	clone := *plan
	clone.Spec = sanitizeSpec(plan.Spec)
	clone.Steps = make([]*PersistedStep, len(plan.Steps))
	for i, step := range plan.Steps {
		stepClone := *step
		stepClone.Spec = sanitizeSpec(step.Spec)
		stepClone.Container = sanitizeContainer(stepClone.Container)
		clone.Steps[i] = &stepClone
	}
	return json.Marshal(&clone)
}

func decodePlan(data []byte) (*PersistedPlan, error) {
	var plan PersistedPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, fmt.Errorf("unmarshal deployment plan: %w", err)
	}
	return &plan, nil
}

// sanitizeSpec returns a copy of spec without environment variables.
func sanitizeSpec(spec api.ServiceSpec) api.ServiceSpec {
	spec.Container.Env = nil
	if spec.PreDeploy != nil {
		spec.PreDeploy.Env = nil
	}
	return spec
}

// sanitizeContainer returns a copy of the container snapshot without environment variables in its Docker config
// or embedded service spec.
func sanitizeContainer(ctr api.ServiceContainer) api.ServiceContainer {
	if ctr.Config != nil {
		ctr.Config.Env = nil
	}
	ctr.ServiceSpec = sanitizeSpec(ctr.ServiceSpec)
	return ctr
}

// isDefinitiveFailure reports whether an error permanently fails a step rather than allowing a resume on rerun.
func isDefinitiveFailure(err error) bool {
	var healthErr *operation.ContainerHealthError
	if errors.As(err, &healthErr) {
		return true
	}
	var hookErr *operation.PreDeployHookError
	return errors.As(err, &hookErr)
}

// specsEqualForResume reports whether a persisted (environment-stripped) spec and a freshly resolved spec
// describe the same deployment, so the running plan can be resumed instead of superseded.
func specsEqualForResume(stored, fresh api.ServiceSpec) bool {
	stored = stored.SetDefaults()
	fresh = fresh.SetDefaults()

	if stored.Mode != fresh.Mode ||
		stored.Name != fresh.Name ||
		stored.Replicas != fresh.Replicas {
		return false
	}
	// Container env is intentionally stripped from stored plans, so compare env separately only when the stored
	// spec actually carries it.
	storedContainer := stored.Container
	freshContainer := fresh.Container
	if storedContainer.Env == nil {
		freshContainer.Env = nil
	}
	if !storedContainer.Equals(freshContainer) {
		return false
	}
	if !cmp.Equal(stored.Placement, fresh.Placement, cmpopts.EquateEmpty()) {
		return false
	}
	if !api.PortsEqual(stored.Ports, fresh.Ports) {
		return false
	}
	if !slices.EqualFunc(stored.Volumes, fresh.Volumes, func(a, b api.VolumeSpec) bool {
		return a.Equals(b)
	}) {
		return false
	}
	if !configsEqual(stored.Configs, fresh.Configs) {
		return false
	}
	if !stored.PreDeploy.Equals(fresh.PreDeploy) {
		return false
	}
	if !stored.Caddy.Equals(fresh.Caddy) {
		return false
	}
	if !cmp.Equal(stored.UpdateConfig, fresh.UpdateConfig, cmpopts.EquateEmpty()) {
		return false
	}
	return true
}

// configsEqual compares two config spec lists irrespective of their order.
func configsEqual(a, b []api.ConfigSpec) bool {
	if len(a) != len(b) {
		return false
	}
	a = slices.Clone(a)
	b = slices.Clone(b)
	slices.SortFunc(a, func(x, y api.ConfigSpec) int {
		if x.Name < y.Name {
			return -1
		}
		if x.Name > y.Name {
			return 1
		}
		return 0
	})
	slices.SortFunc(b, func(x, y api.ConfigSpec) int {
		if x.Name < y.Name {
			return -1
		}
		if x.Name > y.Name {
			return 1
		}
		return 0
	})
	return slices.EqualFunc(a, b, func(x, y api.ConfigSpec) bool { return x.Equals(y) })
}

// stopOptions converts a stop grace period duration to Docker container stop options.
func stepStopOptions(gracePeriod *time.Duration) container.StopOptions {
	if gracePeriod == nil {
		return container.StopOptions{}
	}
	t := int(gracePeriod.Seconds())
	return container.StopOptions{Timeout: &t}
}
