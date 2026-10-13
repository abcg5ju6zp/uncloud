package deploy

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/containerd/errdefs"
	"github.com/docker/compose/v2/pkg/progress"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/pkg/stringid"
	cliprogress "github.com/psviderski/uncloud/internal/cli/progress"
	"github.com/psviderski/uncloud/pkg/api"
	"github.com/psviderski/uncloud/pkg/client/deploy/operation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// buildPersistedSteps converts the in-memory planned operations into stable persisted steps in execution order.
// The participating machine, existing container, volume constraints (through the spec), and update order are
// fixed here and never recomputed during resume.
func buildPersistedSteps(sp *ServicePlan, _ uint64) []*PersistedStep {
	steps := make([]*PersistedStep, 0, len(sp.Operations))
	for _, op := range sp.Operations {
		switch o := op.(type) {
		case *operation.RunContainerOperation:
			steps = append(steps, &PersistedStep{
				Kind:              stepRunContainer,
				Status:            stepPending,
				MachineID:         o.MachineID,
				MachineName:       o.MachineName,
				Spec:              o.Spec,
				SkipHealthMonitor: o.SkipHealthMonitor,
			})
		case *operation.ReplaceContainerOperation:
			steps = append(steps, &PersistedStep{
				Kind:              stepReplaceContainer,
				Status:            stepPending,
				MachineID:         o.MachineID,
				MachineName:       o.MachineName,
				Spec:              o.Spec,
				Order:             o.Order,
				SkipHealthMonitor: o.SkipHealthMonitor,
				StopGracePeriod:   o.StopGracePeriod,
				ContainerID:       o.OldContainer.ID,
				Container:         o.OldContainer,
			})
		case *operation.StopContainerOperation:
			steps = append(steps, &PersistedStep{
				Kind:            stepStopContainer,
				Status:          stepPending,
				MachineID:       o.MachineID,
				MachineName:     o.MachineName,
				StopGracePeriod: o.StopGracePeriod,
				ContainerID:     o.ContainerID,
			})
		case *operation.RemoveContainerOperation:
			steps = append(steps, &PersistedStep{
				Kind:            stepRemoveContainer,
				Status:          stepPending,
				MachineID:       o.MachineID,
				MachineName:     o.MachineName,
				StopGracePeriod: o.StopGracePeriod,
				ContainerID:     o.Container.ID,
				Container:       o.Container,
			})
		case *operation.StopPreDeployOperation:
			steps = append(steps, &PersistedStep{
				Kind:        stepStopPreDeploy,
				Status:      stepPending,
				MachineID:   o.MachineID,
				MachineName: o.MachineName,
				ContainerID: o.Container.ID,
				Container:   o.Container,
			})
		case *operation.RunPreDeployOperation:
			steps = append(steps, &PersistedStep{
				Kind:                stepRunPreDeploy,
				Status:              stepPending,
				MachineID:           o.MachineID,
				MachineName:         o.MachineName,
				Spec:                o.Spec,
				ContainerID:         "",
				OldHookContainerIDs: o.OldContainerIDs,
			})
		default:
			// Unknown operation kinds are not expected from the current strategies; fail loudly rather than
			// silently skip a planned mutation.
			panic(fmt.Sprintf("unsupported deployment operation %T", op))
		}
	}
	return steps
}

// driveSteps executes every not-yet-done step of the plan in its fixed order, persisting state after each
// transition. liveSpec is the freshly resolved spec with secrets, used to create containers; it has the same
// deployment identity as plan.Spec.
func (e *planExecutor) driveSteps(ctx context.Context, plan *PersistedPlan, liveSpec api.ServiceSpec) error {
	for _, step := range plan.Steps {
		if step.Status == stepDone {
			continue
		}
		if err := e.checkFence(ctx, plan); err != nil {
			return err
		}

		if step.Status != stepFailed {
			step.Status = stepRunning
			if err := e.save(ctx, plan); err != nil {
				return err
			}
		}

		var err error
		switch step.Kind {
		case stepRunContainer:
			err = e.runContainerStep(ctx, plan, step, liveSpec)
		case stepReplaceContainer:
			err = e.replaceContainerStep(ctx, plan, step, liveSpec)
		case stepStopContainer:
			err = e.stopContainerStep(ctx, plan, step)
		case stepRemoveContainer:
			err = e.removeContainerStep(ctx, plan, step)
		case stepStopPreDeploy:
			err = e.stopPreDeployStep(ctx, plan, step)
		case stepRunPreDeploy:
			err = e.runPreDeployStep(ctx, plan, step, liveSpec)
		default:
			err = fmt.Errorf("unknown deployment step kind %q", step.Kind)
		}
		if err != nil {
			if errors.Is(err, ErrPlanSuperseded) || errors.Is(err, errContainerOwnedByNewerDeployment) {
				return ErrPlanSuperseded
			}
			return err
		}

		step.Status = stepDone
		step.Error = ""
		if err = e.save(ctx, plan); err != nil {
			return fmt.Errorf("persist completed step %s: %w", step.Kind, err)
		}
	}
	return nil
}

// findServiceContainer locates a service (or hook) container by exact ID or name from the live Docker state
// aggregated from the machines. The second return is false when the container no longer exists.
func (e *planExecutor) findServiceContainer(
	ctx context.Context, plan *PersistedPlan, nameOrID string,
) (api.MachineServiceContainer, bool, error) {
	if nameOrID == "" {
		return api.MachineServiceContainer{}, false, nil
	}
	mc, err := e.client.InspectContainer(ctx, plan.ServiceID, nameOrID)
	if errors.Is(err, api.ErrNotFound) {
		return api.MachineServiceContainer{}, false, nil
	}
	if err != nil {
		return api.MachineServiceContainer{}, false, err
	}
	return mc, true, nil
}

// resolveNewContainer finds the container created by a step, first by its persisted ID and then by its
// deterministic name. The name lookup reconciles a lost creation response where the ID was never persisted.
func (e *planExecutor) resolveNewContainer(
	ctx context.Context, plan *PersistedPlan, step *PersistedStep,
) (api.MachineServiceContainer, bool, error) {
	if step.NewContainerID != "" {
		mc, found, err := e.findServiceContainer(ctx, plan, step.NewContainerID)
		if err != nil || found {
			return mc, found, err
		}
	}
	if step.NewContainerName != "" {
		return e.findServiceContainer(ctx, plan, step.NewContainerName)
	}
	return api.MachineServiceContainer{}, false, nil
}

// deploymentContext returns a context carrying the plan identity and an optional deterministic container name.
func deploymentContext(ctx context.Context, plan *PersistedPlan, containerName string) context.Context {
	return api.WithDeploymentIdentity(ctx, api.DeploymentIdentity{
		PlanID:        plan.ID,
		Generation:    plan.Generation,
		ContainerName: containerName,
	})
}

// planShortID returns a short hex segment of the plan ID safe to embed in Docker container names.
func planShortID(plan *PersistedPlan) string {
	if len(plan.ID) >= 8 {
		return plan.ID[:8]
	}
	return plan.ID
}

// containerEventCtx overrides the progress event ID so start and health actions share the creation event line.
func containerEventCtx(ctx context.Context, containerName, machineName string) context.Context {
	return cliprogress.WithEventID(ctx,
		cliprogress.NewContainerEventID(ctx, containerName, machineName))
}

// newContainerName returns the deterministic Docker name for a container created by a run/replace step.
func newContainerName(plan *PersistedPlan, step *PersistedStep) string {
	return fmt.Sprintf("%s-%s-%d", plan.ServiceName, planShortID(plan), step.index(plan))
}

// newHookContainerName returns the deterministic Docker name for a pre-deploy hook container created by a step.
func newHookContainerName(plan *PersistedPlan, step *PersistedStep) string {
	return fmt.Sprintf("%s-%s-%s-%d",
		plan.ServiceName, api.LabelHookPreDeploy, planShortID(plan), step.index(plan))
}

// index returns the position of the step in its plan.
func (s *PersistedStep) index(plan *PersistedPlan) int {
	for i, candidate := range plan.Steps {
		if candidate == s {
			return i
		}
	}
	return 0
}

// createNewContainer creates the container for a run/replace step under the plan identity and persists the new
// container ID immediately, then re-inspects it from live Docker state to reconcile a potentially lost response.
func (e *planExecutor) createNewContainer(
	ctx context.Context, plan *PersistedPlan, step *PersistedStep, liveSpec api.ServiceSpec, hook bool, name string,
) (api.MachineServiceContainer, error) {
	createCtx := deploymentContext(ctx, plan, name)
	if hook {
		createCtx = cliprogress.WithEventID(createCtx,
			cliprogress.PreDeployHookEventID(plan.ServiceName, step.MachineName))
	}

	if err := e.checkFence(createCtx, plan); err != nil {
		return api.MachineServiceContainer{}, err
	}

	var (
		resp api.CreateContainerResponse
		err  error
	)
	if hook {
		resp, err = e.client.CreatePreDeployHookContainer(createCtx, plan.ServiceID, liveSpec, step.MachineID)
	} else {
		resp, err = e.client.CreateContainer(createCtx, plan.ServiceID, liveSpec, step.MachineID)
	}
	if err != nil {
		// The create call may have succeeded on Docker while its response was lost. Reconcile by looking the
		// deterministic container name up before failing, so the step is retried at most once and never creates
		// a duplicate container.
		if adopted, found, adoptErr := e.findServiceContainer(ctx, plan, name); adoptErr == nil && found {
			step.NewContainerID = adopted.Container.ID
			step.NewContainerName = name
			if step.Phase == "" {
				step.Phase = phaseCreated
			}
			if saveErr := e.save(ctx, plan); saveErr != nil {
				return api.MachineServiceContainer{}, fmt.Errorf("persist adopted container: %w", saveErr)
			}
			return adopted, nil
		}
		return api.MachineServiceContainer{}, fmt.Errorf("create container: %w", err)
	}

	// Persist before doing anything else: even if the next call or the client itself dies, a resume can adopt
	// this container by ID or deterministic name instead of creating a duplicate.
	step.NewContainerID = resp.ID
	step.NewContainerName = resp.Name
	if step.Phase == "" {
		step.Phase = phaseCreated
	}
	if err = e.save(ctx, plan); err != nil {
		return api.MachineServiceContainer{}, fmt.Errorf("persist created container: %w", err)
	}

	mc, found, err := e.findServiceContainer(ctx, plan, resp.ID)
	if err != nil {
		return api.MachineServiceContainer{}, err
	}
	if !found {
		return api.MachineServiceContainer{}, fmt.Errorf("created container %s is not reported by any machine",
			stringid.TruncateID(resp.ID))
	}
	return mc, nil
}

// startNewContainer starts the created container and persists the started phase.
func (e *planExecutor) startNewContainer(
	ctx context.Context, plan *PersistedPlan, step *PersistedStep, ctrID string, eventCtx context.Context,
) error {
	if err := e.checkFence(eventCtx, plan); err != nil {
		return err
	}
	if err := e.client.StartContainer(eventCtx, plan.ServiceID, ctrID); err != nil {
		return fmt.Errorf("start container: %w", err)
	}
	step.Phase = phaseStarted
	return e.save(ctx, plan)
}

// confirmHealthy verifies the new container is healthy. When startedByUs is false the container was already
// running when the plan resumed (possibly across a client restart), so its current Docker state is trusted
// instead of re-running the full monitor period: a stayed-up container without a health check is considered
// healthy immediately, and a health-checked container still returns as soon as Docker reports healthy.
func (e *planExecutor) confirmHealthy(
	ctx context.Context, plan *PersistedPlan, step *PersistedStep, ctrID string, startedByUs bool,
) error {
	if step.SkipHealthMonitor {
		return nil
	}

	if !startedByUs {
		mc, found, err := e.findServiceContainer(ctx, plan, ctrID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("container %s disappeared before health confirmation", stringid.TruncateID(ctrID))
		}
		switch {
		case mc.Container.Healthy():
			return nil
		case !mc.Container.State.Running:
			humanState, _ := mc.Container.HumanState()
			return fmt.Errorf("container is not running after interrupted deployment (%s)", humanState)
		case !mc.Container.HasHealthcheck():
			// It stayed running across the interruption and has no health check to wait for.
			return nil
		}
		// A health-checked container that is still starting falls through to the regular wait, which returns as
		// soon as Docker reports healthy.
	}

	opts := api.WaitContainerHealthyOptions{MonitorPeriod: step.Spec.UpdateConfig.MonitorPeriod}
	return e.client.WaitContainerHealthy(ctx, plan.ServiceID, ctrID, opts)
}

// newContainerHealthError wraps a health confirmation failure in the same error type the non-durable operations
// use, so the CLI can surface and fetch logs of the failed container.
func newContainerHealthError(plan *PersistedPlan, step *PersistedStep, ctrID string, err error) error {
	return operation.NewContainerHealthError(
		fmt.Errorf("new container '%s/%s' failed to become healthy: %w",
			plan.ServiceName, stringid.TruncateID(ctrID), err),
		plan.ServiceName, ctrID, step.MachineName)
}

// runContainerStep reconciles a run-container step: create (or adopt), start, confirm health.
func (e *planExecutor) runContainerStep(
	ctx context.Context, plan *PersistedPlan, step *PersistedStep, liveSpec api.ServiceSpec,
) error {
	name := newContainerName(plan, step)

	mc, found, err := e.resolveNewContainer(ctx, plan, step)
	if err != nil {
		return err
	}
	if !found && step.Phase != "" {
		// A container we previously recorded has vanished (e.g. removed out of band). Reset and recreate unless
		// the service generation has moved, in which case the fence check below ends the plan.
		step.NewContainerID = ""
		step.NewContainerName = ""
		step.Phase = ""
	}

	startedByUs := false
	if !found {
		mc, err = e.createNewContainer(ctx, plan, step, liveSpec, false, name)
		if err != nil {
			return err
		}
	}

	eventCtx := containerEventCtx(ctx, mc.Container.Name, step.MachineName)
	if !mc.Container.State.Running {
		if err = e.startNewContainer(ctx, plan, step, mc.Container.ID, eventCtx); err != nil {
			return err
		}
		startedByUs = true
	} else if step.Phase == "" || step.Phase == phaseCreated {
		// The start response was lost in a previous attempt but the container is running.
		step.Phase = phaseStarted
		if err = e.save(ctx, plan); err != nil {
			return err
		}
	}

	if step.Phase != phaseHealthy {
		if err = e.confirmHealthy(ctx, plan, step, mc.Container.ID, startedByUs); err != nil {
			return newContainerHealthError(plan, step, mc.Container.ID, err)
		}
		step.Phase = phaseHealthy
		if err = e.save(ctx, plan); err != nil {
			return err
		}
	}
	return nil
}

// replaceContainerStep reconciles a replace-container step in the fixed update order:
// create new, (stop-first: stop old), start new, confirm healthy, stop old, remove old.
func (e *planExecutor) replaceContainerStep(
	ctx context.Context, plan *PersistedPlan, step *PersistedStep, liveSpec api.ServiceSpec,
) error {
	stopFirst := step.Order == api.UpdateOrderStopFirst
	name := newContainerName(plan, step)

	mc, found, err := e.resolveNewContainer(ctx, plan, step)
	if err != nil {
		return err
	}
	if !found && step.Phase != "" {
		// The new container we recorded has vanished (e.g. removed out of band). Only the new-container state is
		// reset: WasRunning and OldStopped describe the old container and must survive so a stop-first rollback
		// decision stays correct. The fence check before recreating ends the plan if a newer generation moved on.
		step.NewContainerID = ""
		step.NewContainerName = ""
		step.Phase = ""
	}
	if !found {
		mc, err = e.createNewContainer(ctx, plan, step, liveSpec, false, name)
		if err != nil {
			return err
		}
	}
	newCtr := mc.Container
	newEventCtx := containerEventCtx(ctx, newCtr.Name, step.MachineName)

	// Stop-first cutover: stop the old container before starting the new one.
	if stopFirst && !step.OldStopped {
		oldMC, oldFound, oldErr := e.findServiceContainer(ctx, plan, step.ContainerID)
		if oldErr != nil {
			return fmt.Errorf("inspect old container: %w", oldErr)
		}
		if oldFound {
			if err = ensurePlanOwnsContainer(oldMC.Container, plan); err != nil {
				return err
			}
			step.WasRunning = oldMC.Container.State.Running || step.WasRunning
			if oldMC.Container.State.Running {
				if err = e.stopWithFence(newEventCtx, plan, step, oldMC.Container.ID); err != nil {
					return fmt.Errorf("stop old container: %w", err)
				}
			}
		} else {
			// The old container is already gone. Remember it was running only if the snapshot captured at
			// planning time says so, so a rollback decision stays consistent.
			step.WasRunning = step.WasRunning || step.Container.State.Running
		}
		step.OldStopped = true
		if err = e.save(ctx, plan); err != nil {
			return err
		}
	}

	// Start the new container.
	startedByUs := false
	if !newCtr.State.Running {
		if err = e.startNewContainer(ctx, plan, step, newCtr.ID, newEventCtx); err != nil {
			return err
		}
		startedByUs = true
	} else if step.Phase == "" || step.Phase == phaseCreated {
		step.Phase = phaseStarted
		if err = e.save(ctx, plan); err != nil {
			return err
		}
	}

	// Confirm health, rolling back to the old container on failure (stop-first only, mirroring the operation).
	if step.Phase != phaseHealthy {
		if err = e.confirmHealthy(ctx, plan, step, newCtr.ID, startedByUs); err != nil {
			return e.replaceHealthFailure(ctx, plan, step, newCtr.ID, stopFirst, err)
		}
		step.Phase = phaseHealthy
		if err = e.save(ctx, plan); err != nil {
			return err
		}
	}

	// Start-first cutover: stop the old container only after the new one is healthy.
	if !stopFirst && !step.OldStopped {
		oldMC, oldFound, oldErr := e.findServiceContainer(ctx, plan, step.ContainerID)
		if oldErr != nil {
			return fmt.Errorf("inspect old container: %w", oldErr)
		}
		if oldFound {
			if err = ensurePlanOwnsContainer(oldMC.Container, plan); err != nil {
				return err
			}
			if oldMC.Container.State.Running {
				if err = e.stopWithFence(ctx, plan, step, oldMC.Container.ID); err != nil {
					return fmt.Errorf("stop old container: %w", err)
				}
			}
		}
		step.OldStopped = true
		if err = e.save(ctx, plan); err != nil {
			return err
		}
	}

	// Remove the old container.
	oldMC, oldFound, err := e.findServiceContainer(ctx, plan, step.ContainerID)
	if err != nil {
		return fmt.Errorf("inspect old container: %w", err)
	}
	if oldFound {
		if err = ensurePlanOwnsContainer(oldMC.Container, plan); err != nil {
			return err
		}
		if err = e.removeWithFence(ctx, plan, step, oldMC.Container.ID); err != nil {
			return fmt.Errorf("remove old container: %w", err)
		}
	}
	return nil
}

// replaceHealthFailure stops the unhealthy new container, rolls back to the old one for stop-first replacements
// when it was previously running, and returns a definitive ContainerHealthError.
func (e *planExecutor) replaceHealthFailure(
	ctx context.Context, plan *PersistedPlan, step *PersistedStep, newCtrID string, stopFirst bool, healthErr error,
) error {
	// Use a context without progress so stopping the failed container doesn't overwrite its status line.
	ctxWithoutProgress := progress.WithContextWriter(ctx, nil)
	_ = e.client.StopContainer(ctxWithoutProgress, plan.ServiceID, newCtrID, stepStopOptions(step.StopGracePeriod))

	finalErr := newContainerHealthError(plan, step, newCtrID, healthErr)
	if stopFirst && step.WasRunning && !step.RolledBack {
		oldCtr := fmt.Sprintf("%s/%s", plan.ServiceName, stringid.TruncateID(step.ContainerID))
		if rollbackErr := e.client.StartContainer(ctx, plan.ServiceID, step.ContainerID); rollbackErr != nil {
			finalErr = operation.NewContainerHealthError(
				fmt.Errorf("%w. Rolled back to old container '%s' but failed to restart it: %w",
					finalErr, oldCtr, rollbackErr),
				plan.ServiceName, newCtrID, step.MachineName)
		} else {
			finalErr = operation.NewContainerHealthError(
				fmt.Errorf("%w. Rolled back to old container '%s'", finalErr, oldCtr),
				plan.ServiceName, newCtrID, step.MachineName)
			step.RolledBack = true
			_ = e.save(context.WithoutCancel(ctx), plan)
		}
	}
	return finalErr
}

// stopWithFence checks the generation and stops a container honoring the step grace period.
func (e *planExecutor) stopWithFence(
	ctx context.Context, plan *PersistedPlan, step *PersistedStep, ctrID string,
) error {
	if err := e.checkFence(ctx, plan); err != nil {
		return err
	}
	return e.client.StopContainer(ctx, plan.ServiceID, ctrID, stepStopOptions(step.StopGracePeriod))
}

// removeWithFence checks the generation, stops a running container gracefully, and removes it with its anonymous
// volumes. A container that is already gone is treated as success.
func (e *planExecutor) removeWithFence(
	ctx context.Context, plan *PersistedPlan, step *PersistedStep, ctrID string,
) error {
	if err := e.checkFence(ctx, plan); err != nil {
		return err
	}
	mc, found, err := e.findServiceContainer(ctx, plan, ctrID)
	if err != nil {
		return err
	}
	if found && mc.Container.State.Running {
		if err = e.client.StopContainer(ctx, plan.ServiceID, ctrID, stepStopOptions(step.StopGracePeriod)); err != nil {
			return fmt.Errorf("stop container: %w", err)
		}
	}
	err = e.client.RemoveContainer(ctx, plan.ServiceID, ctrID, container.RemoveOptions{RemoveVolumes: true})
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove container: %w", err)
	}
	return nil
}

// stopContainerStep stops an existing container unless it is already stopped or gone.
func (e *planExecutor) stopContainerStep(
	ctx context.Context, plan *PersistedPlan, step *PersistedStep,
) error {
	mc, found, err := e.findServiceContainer(ctx, plan, step.ContainerID)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if err = ensurePlanOwnsContainer(mc.Container, plan); err != nil {
		return err
	}
	if !mc.Container.State.Running {
		return nil
	}
	return e.stopWithFence(ctx, plan, step, mc.Container.ID)
}

// removeContainerStep stops and removes an existing container unless it is already gone.
func (e *planExecutor) removeContainerStep(
	ctx context.Context, plan *PersistedPlan, step *PersistedStep,
) error {
	mc, found, err := e.findServiceContainer(ctx, plan, step.ContainerID)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if err = ensurePlanOwnsContainer(mc.Container, plan); err != nil {
		return err
	}
	return e.removeWithFence(ctx, plan, step, mc.Container.ID)
}

// stopPreDeployStep stops a running pre-deploy hook container from a previous deployment.
func (e *planExecutor) stopPreDeployStep(
	ctx context.Context, plan *PersistedPlan, step *PersistedStep,
) error {
	mc, found, err := e.findServiceContainer(ctx, plan, step.ContainerID)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if err = ensurePlanOwnsContainer(mc.Container, plan); err != nil {
		return err
	}
	if !mc.Container.State.Running {
		return nil
	}

	stopCtx := cliprogress.WithEventID(ctx,
		cliprogress.OldPreDeployHookEventID(plan.ServiceName, mc.Container.ID, step.MachineName))
	if err = e.checkFence(stopCtx, plan); err != nil {
		return err
	}
	if err = e.client.StopContainer(stopCtx, plan.ServiceID, mc.Container.ID, container.StopOptions{}); err != nil {
		if isNotFound(err) {
			return nil
		}
		return fmt.Errorf("stop pre-deploy hook container: %w", err)
	}
	return nil
}

// runPreDeployStep reconciles a pre-deploy hook step: remove previous hook containers, create and start the new
// one, and wait for it to exit successfully. A hook that has already exited successfully is never executed twice.
func (e *planExecutor) runPreDeployStep(
	ctx context.Context, plan *PersistedPlan, step *PersistedStep, liveSpec api.ServiceSpec,
) error {
	// Remove old pre-deploy hook containers recorded at planning time.
	for _, oldID := range step.OldHookContainerIDs {
		if containsString(step.RemovedHookIDs, oldID) {
			continue
		}
		oldMC, found, err := e.findServiceContainer(ctx, plan, oldID)
		if err != nil {
			return err
		}
		if found {
			if err = ensurePlanOwnsContainer(oldMC.Container, plan); err != nil {
				return err
			}
			oldCtx := cliprogress.WithEventID(ctx,
				cliprogress.OldPreDeployHookEventID(plan.ServiceName, oldID, step.MachineName))
			if err = e.checkFence(oldCtx, plan); err != nil {
				return err
			}
			_ = e.client.StopContainer(oldCtx, plan.ServiceID, oldID, container.StopOptions{})
			if err = e.client.RemoveContainer(oldCtx, plan.ServiceID, oldID,
				container.RemoveOptions{RemoveVolumes: true}); err != nil && !isNotFound(err) {
				return fmt.Errorf("remove old pre-deploy hook container: %w", err)
			}
		}
		step.RemovedHookIDs = append(step.RemovedHookIDs, oldID)
		if err = e.save(ctx, plan); err != nil {
			return err
		}
	}

	name := newHookContainerName(plan, step)
	mc, found, err := e.resolveNewContainer(ctx, plan, step)
	if err != nil {
		return err
	}
	if !found && step.Phase != "" {
		step.NewContainerID = ""
		step.NewContainerName = ""
		step.Phase = ""
	}

	hookCtx := cliprogress.WithEventID(ctx,
		cliprogress.PreDeployHookEventID(plan.ServiceName, step.MachineName))

	startedByUs := false
	if !found {
		mc, err = e.createNewContainer(ctx, plan, step, liveSpec, true, name)
		if err != nil {
			return err
		}
	}

	// A hook that already exited (from a previous attempt) is reconciled from its exit code: never rerun a
	// successful hook, and surface a failed hook as a definitive failure. FinishedAt distinguishes an exited
	// container from one that was created but never started (Docker reports the zero time otherwise).
	if !mc.Container.State.Running && containerHasExited(mc.Container) {
		if mc.Container.State.ExitCode == 0 {
			return nil
		}
		return e.preDeployHookFailure(plan, step, mc.Container.ID,
			fmt.Errorf("pre-deploy hook container exited with code %d before it could be confirmed",
				mc.Container.State.ExitCode))
	}

	if !mc.Container.State.Running {
		if err = e.checkFence(hookCtx, plan); err != nil {
			return err
		}
		if err = e.client.StartContainer(hookCtx, plan.ServiceID, mc.Container.ID); err != nil {
			return fmt.Errorf("start pre-deploy hook container: %w", err)
		}
		startedByUs = true
		step.Phase = phaseStarted
		if err = e.save(ctx, plan); err != nil {
			return err
		}
	}

	if err = e.waitPreDeployHookExit(hookCtx, plan, step, mc.Container.ID, liveSpec, startedByUs); err != nil {
		return err
	}
	return nil
}

// preDeployHookTimeout returns the configured hook timeout or the default.
func preDeployHookTimeout(spec api.ServiceSpec) time.Duration {
	if spec.PreDeploy != nil && spec.PreDeploy.Timeout != nil {
		return *spec.PreDeploy.Timeout
	}
	return operation.DefaultPreDeployTimeout
}

// waitPreDeployHookExit polls the hook container until it exits successfully, fails, or times out. On resume the
// remaining timeout is computed from the container start time so the hook can't run forever across restarts.
func (e *planExecutor) waitPreDeployHookExit(
	ctx context.Context, plan *PersistedPlan, step *PersistedStep, ctrID string,
	liveSpec api.ServiceSpec, startedByUs bool,
) error {
	pw := progress.ContextWriter(ctx)
	eventID := cliprogress.PreDeployHookEventID(plan.ServiceName, step.MachineName)

	timeout := preDeployHookTimeout(liveSpec)
	deadline := time.Now().Add(timeout)
	if !startedByUs {
		mc, found, err := e.findServiceContainer(ctx, plan, ctrID)
		if err != nil {
			return err
		}
		if found {
			if startedAt, parseErr := time.Parse(time.RFC3339Nano, mc.Container.State.StartedAt); parseErr == nil {
				deadline = startedAt.Add(timeout)
			}
		}
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				// Stop the timed-out hook and fail definitively.
				_ = e.client.StopContainer(progress.WithContextWriter(ctx, nil),
					plan.ServiceID, ctrID, container.StopOptions{})
				pw.Event(progress.NewEvent(eventID, progress.Error, fmt.Sprintf("Timeout (%s)", timeout)))
				return e.preDeployHookFailure(plan, step, ctrID,
					fmt.Errorf("pre-deploy hook container timed out after %s. "+
						"It's stopped and available for inspection. View logs with 'uc logs %s'",
						timeout, plan.ServiceName))
			}

			mc, found, err := e.findServiceContainer(ctx, plan, ctrID)
			if err != nil {
				return fmt.Errorf("inspect pre-deploy hook container: %w", err)
			}
			if !found {
				return fmt.Errorf("pre-deploy hook container %s disappeared while waiting for it to exit",
					stringid.TruncateID(ctrID))
			}

			if mc.Container.State.Running {
				pw.Event(progress.Waiting(eventID))
				continue
			}
			if mc.Container.State.ExitCode == 0 {
				pw.Event(progress.Event{ID: eventID, Status: progress.Done})
				return nil
			}
			pw.Event(progress.ErrorEvent(eventID))
			return e.preDeployHookFailure(plan, step, ctrID,
				fmt.Errorf("pre-deploy hook container failed with exit code: %d. "+
					"It's stopped and available for inspection. View logs with 'uc logs %s'",
					mc.Container.State.ExitCode, plan.ServiceName))
		}
	}
}

// preDeployHookFailure builds a definitive PreDeployHookError matching the non-durable operation error type.
func (e *planExecutor) preDeployHookFailure(
	plan *PersistedPlan, step *PersistedStep, ctrID string, err error,
) error {
	return operation.NewPreDeployHookError(
		fmt.Errorf("pre-deploy hook container '%s/%s': %w", plan.ServiceName,
			stringid.TruncateID(ctrID), err),
		plan.ServiceName, ctrID, step.MachineName)
}

// containerHasExited reports whether a container has reached the exited state at least once, based on Docker's
// state status ("exited"/"dead"). A merely created-but-never-started container has status "created".
func containerHasExited(ctr api.ServiceContainer) bool {
	return ctr.State.Status == "exited" || ctr.State.Status == "dead"
}

// containsString reports whether the slice contains the value.
func containsString(slice []string, value string) bool {
	for _, item := range slice {
		if item == value {
			return true
		}
	}
	return false
}

// isNotFound reports whether an error represents a Docker/gRPC container that no longer exists.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errdefs.IsNotFound(err) {
		return true
	}
	return status.Code(err) == codes.NotFound
}
