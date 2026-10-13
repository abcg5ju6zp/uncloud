package deploy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/psviderski/uncloud/pkg/api"
	"github.com/psviderski/uncloud/pkg/client/deploy/operation"
)

const (
	execTestServiceID      = "0123456789abcdef0123456789abcdef"
	execTestMachineID      = "11111111111111111111111111111111"
	execTestMachineName    = "machine-1"
	testAppName            = "app"
	testAppOldName         = "app-old"
	testAppNewName         = "app-new"
	testHookName           = "app-hook"
	containerStatusCreated = "created"
	containerStatusRunning = "running"
	containerStatusExited  = "exited"
)

func execTestSpec(image string) api.ServiceSpec {
	spec := api.ServiceSpec{
		Name:     testAppName,
		Mode:     api.ServiceModeReplicated,
		Replicas: 1,
		Container: api.ContainerSpec{
			Image: image,
			Env:   api.EnvVars{"SECRET_TOKEN": "super-secret"},
		},
	}
	return spec.SetDefaults()
}

// fakeContainer is a minimal model of a Docker container for the executor tests.
type fakeContainer struct {
	id        string
	name      string
	running   bool
	status    string
	exitCode  int
	labels    map[string]string
	hook      bool
	startedAt time.Time
}

// fakeExecClient models the cluster client: an in-memory deployment plan/generation store and Docker containers.
type fakeExecClient struct {
	api.ContainerClient
	api.VolumeClient

	mu sync.Mutex

	containers    map[string]*fakeContainer // by ID
	idToName      map[string]string
	nameToID      map[string]map[string]string // service -> name -> ID
	plans         map[string]map[string][]byte
	generations   map[string]uint64
	calls         []string
	nextID        int
	lostCreate    bool // the next create loses its response after the container appears
	stopErrors    map[string]error
	waitFailUntil map[string]int // container ID -> number of wait calls after which it starts succeeding
	waitCalls     map[string]int
	genOverride   uint64 // when non-zero, GetServiceDeploymentGeneration returns this
}

func newFakeExecClient() *fakeExecClient {
	return &fakeExecClient{
		containers:    make(map[string]*fakeContainer),
		idToName:      make(map[string]string),
		nameToID:      make(map[string]map[string]string),
		plans:         make(map[string]map[string][]byte),
		generations:   make(map[string]uint64),
		stopErrors:    make(map[string]error),
		waitFailUntil: make(map[string]int),
		waitCalls:     make(map[string]int),
	}
}

func (c *fakeExecClient) allocID() string {
	c.nextID++
	return fmt.Sprintf("%032d", c.nextID)
}

func (c *fakeExecClient) record(ctr *fakeContainer, serviceID string) {
	c.containers[ctr.id] = ctr
	c.idToName[ctr.id] = ctr.name
	byName := c.nameToID[serviceID]
	if byName == nil {
		byName = make(map[string]string)
		c.nameToID[serviceID] = byName
	}
	byName[ctr.name] = ctr.id
}

func (c *fakeExecClient) create(ctx context.Context, hook bool, machineID string) (api.CreateContainerResponse, error) {
	identity, _ := api.DeploymentIdentityFromContext(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls = append(c.calls, map[bool]string{true: "create:hook", false: "create:service"}[hook])

	id := c.allocID()
	name := identity.ContainerName
	if name == "" {
		name = fmt.Sprintf("app-random-%d", c.nextID)
	}
	ctr := &fakeContainer{
		id:      id,
		name:    name,
		running: false,
		status:  containerStatusCreated,
		labels: map[string]string{
			api.LabelServiceID:   execTestServiceID,
			api.LabelServiceName: testAppName,
			api.LabelManaged:     "",
		},
	}
	if identity.PlanID != "" {
		ctr.labels[api.LabelDeploymentID] = identity.PlanID
	}
	if identity.Generation > 0 {
		ctr.labels[api.LabelDeploymentGeneration] = fmt.Sprintf("%d", identity.Generation)
	}
	if hook {
		ctr.labels[api.LabelHook] = api.LabelHookPreDeploy
		ctr.hook = true
	}
	c.record(ctr, execTestServiceID)

	lost := c.lostCreate
	c.lostCreate = false
	if lost {
		return api.CreateContainerResponse{}, errors.New("rpc response lost (simulated)")
	}
	return api.CreateContainerResponse{
		CreateResponse: container.CreateResponse{ID: id},
		Name:           name,
	}, nil
}

func (c *fakeExecClient) CreateContainer(
	ctx context.Context, _ string, _ api.ServiceSpec, _ string,
) (api.CreateContainerResponse, error) {
	return c.create(ctx, false, "")
}

func (c *fakeExecClient) CreatePreDeployHookContainer(
	ctx context.Context, _ string, _ api.ServiceSpec, _ string,
) (api.CreateContainerResponse, error) {
	return c.create(ctx, true, "")
}

func (c *fakeExecClient) InspectContainer(
	_ context.Context, serviceNameOrID, nameOrID string,
) (api.MachineServiceContainer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	id := nameOrID
	if byName := c.nameToID[serviceNameOrID]; byName != nil {
		if named, ok := byName[nameOrID]; ok {
			id = named
		}
	}
	ctr, ok := c.containers[id]
	if !ok {
		return api.MachineServiceContainer{}, api.ErrNotFound
	}
	return api.MachineServiceContainer{
		MachineID:   execTestMachineID,
		MachineName: execTestMachineName,
		Container:   ctr.toServiceContainer(),
	}, nil
}

func (c *fakeExecClient) StartContainer(_ context.Context, _, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls = append(c.calls, "start:"+shortFakeID(id))
	ctr, ok := c.containers[id]
	if !ok {
		return api.ErrNotFound
	}
	ctr.running = true
	ctr.status = containerStatusRunning
	ctr.startedAt = time.Now()
	return nil
}

func (c *fakeExecClient) StopContainer(_ context.Context, _, id string, _ container.StopOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls = append(c.calls, "stop:"+shortFakeID(id))
	ctr, ok := c.containers[id]
	if !ok {
		return api.ErrNotFound
	}
	ctr.running = false
	ctr.status = containerStatusExited
	ctr.exitCode = 0
	return nil
}

func (c *fakeExecClient) RemoveContainer(
	_ context.Context, _, id string, _ container.RemoveOptions,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls = append(c.calls, "remove:"+shortFakeID(id))
	ctr, ok := c.containers[id]
	if !ok {
		return api.ErrNotFound
	}
	delete(c.containers, id)
	delete(c.idToName, id)
	for _, byName := range c.nameToID {
		delete(byName, ctr.name)
	}
	return nil
}

func (c *fakeExecClient) WaitContainerHealthy(
	_ context.Context, _, id string, _ api.WaitContainerHealthyOptions,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls = append(c.calls, "wait:"+shortFakeID(id))
	c.waitCalls[id]++

	if until := c.waitFailUntil[id]; c.waitCalls[id] <= until {
		return errors.New("not healthy yet (simulated)")
	}
	ctr, ok := c.containers[id]
	if !ok {
		return api.ErrNotFound
	}
	if !ctr.running {
		return errors.New("container is not running (simulated)")
	}
	return nil
}

// Plan store implementation.

func (c *fakeExecClient) GetDeploymentPlan(
	_ context.Context, serviceID, planID string,
) (api.DeploymentPlanRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if byID := c.plans[serviceID]; byID != nil {
		if data, ok := byID[planID]; ok {
			return api.DeploymentPlanRecord{ID: planID, Data: data}, nil
		}
	}
	return api.DeploymentPlanRecord{}, api.ErrNotFound
}

func (c *fakeExecClient) PutDeploymentPlan(
	_ context.Context, serviceID, planID string, data []byte,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.plans[serviceID] == nil {
		c.plans[serviceID] = make(map[string][]byte)
	}
	c.plans[serviceID][planID] = append([]byte(nil), data...)
	return nil
}

func (c *fakeExecClient) ListDeploymentPlans(
	_ context.Context, serviceID string,
) ([]api.DeploymentPlanRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var records []api.DeploymentPlanRecord
	for id, data := range c.plans[serviceID] {
		records = append(records, api.DeploymentPlanRecord{ID: id, Data: data})
	}
	return records, nil
}

func (c *fakeExecClient) DeleteDeploymentPlan(_ context.Context, serviceID, planID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.plans[serviceID], planID)
	return nil
}

func (c *fakeExecClient) GetServiceDeploymentGeneration(
	_ context.Context, serviceID string,
) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.genOverride > 0 {
		return c.genOverride, nil
	}
	return c.generations[serviceID], nil
}

func (c *fakeExecClient) AdvanceServiceDeploymentGeneration(
	_ context.Context, serviceID string,
) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generations[serviceID]++
	return c.generations[serviceID], nil
}

func (c *fakeExecClient) storedPlan(t *testing.T, serviceID string) *PersistedPlan {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.NotEmpty(t, c.plans[serviceID], "expected a persisted plan")
	var plan *PersistedPlan
	for id, data := range c.plans[serviceID] {
		decoded, err := decodePlan(data)
		require.NoError(t, err)
		decoded.ID = id
		plan = decoded
	}
	return plan
}

func (c *fakeExecClient) storePlan(plan *PersistedPlan) {
	data, err := encodePlan(plan)
	if err != nil {
		panic(err)
	}
	_ = c.PutDeploymentPlan(context.Background(), plan.ServiceID, plan.ID, data)
}

func (f *fakeContainer) toServiceContainer() api.ServiceContainer {
	state := &container.State{
		Status:    f.status,
		Running:   f.running,
		ExitCode:  f.exitCode,
		StartedAt: "0001-01-01T00:00:00Z",
	}
	if !f.startedAt.IsZero() {
		state.StartedAt = f.startedAt.UTC().Format(time.RFC3339Nano)
		if !f.running {
			state.FinishedAt = f.startedAt.Add(time.Second).UTC().Format(time.RFC3339Nano)
		}
	}
	return api.ServiceContainer{
		Container: api.Container{InspectResponse: container.InspectResponse{
			ContainerJSONBase: &container.ContainerJSONBase{
				ID:    f.id,
				Name:  f.name,
				State: state,
			},
			Config: &container.Config{Labels: f.labels},
		}},
	}
}

func shortFakeID(id string) string {
	if len(id) > 4 {
		return id[len(id)-4:]
	}
	return id
}

func runServicePlan(t *testing.T, cli *fakeExecClient, sp *ServicePlan) error {
	t.Helper()
	return sp.Execute(context.Background(), cli)
}

func TestExecutorRunContainerHappyPath(t *testing.T) {
	t.Parallel()

	cli := newFakeExecClient()
	spec := execTestSpec("nginx:1")
	sp := &ServicePlan{
		ServiceID:   execTestServiceID,
		ServiceName: testAppName,
		Spec:        spec,
		SequenceOperation: operation.SequenceOperation{Operations: []operation.Operation{
			&operation.RunContainerOperation{
				ServiceID:   execTestServiceID,
				Spec:        spec,
				MachineID:   execTestMachineID,
				MachineName: execTestMachineName,
			},
		}},
	}

	require.NoError(t, runServicePlan(t, cli, sp))

	plan := cli.storedPlan(t, execTestServiceID)
	assert.Equal(t, planStatusCompleted, plan.Status)
	assert.EqualValues(t, 1, plan.Generation)
	require.Len(t, plan.Steps, 1)
	assert.Equal(t, stepDone, plan.Steps[0].Status)
	assert.NotEmpty(t, plan.Steps[0].NewContainerID)
	assert.Equal(t, phaseHealthy, plan.Steps[0].Phase)

	// Exactly one service container exists and it carries the deployment identity labels.
	require.Len(t, cli.containers, 1)
	for _, ctr := range cli.containers {
		assert.Equal(t, plan.ID, ctr.labels[api.LabelDeploymentID])
		assert.Equal(t, "1", ctr.labels[api.LabelDeploymentGeneration])
	}

	// Secrets must not leak into the replicated plan record.
	for _, data := range cli.plans[execTestServiceID] {
		assert.NotContains(t, string(data), "super-secret")
	}

	// A rerun with the same spec is a no-op: no new generation is allocated.
	again := &ServicePlan{
		ServiceID:   execTestServiceID,
		ServiceName: testAppName,
		Spec:        spec,
	}
	require.NoError(t, runServicePlan(t, cli, again))
	gen, err := cli.GetServiceDeploymentGeneration(context.Background(), execTestServiceID)
	require.NoError(t, err)
	assert.EqualValues(t, 1, gen)
}

func TestExecutorResumeReplacesWithoutDuplicate(t *testing.T) {
	t.Parallel()

	// Simulate the exact interruption from the issue: the new container is healthy but the old container has not
	// been stopped or removed yet when the client disconnects.
	cli := newFakeExecClient()
	spec := execTestSpec("nginx:2")

	oldID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	newID := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	cli.record(&fakeContainer{
		id: oldID, name: testAppOldName, running: true, status: containerStatusRunning,
		labels: map[string]string{
			api.LabelServiceID:            execTestServiceID,
			api.LabelServiceName:          testAppName,
			api.LabelDeploymentGeneration: "1", // belongs to an earlier deployment than the plan
		},
	}, execTestServiceID)
	cli.record(&fakeContainer{
		id: newID, name: testAppNewName, running: true, status: containerStatusRunning,
		labels: map[string]string{
			api.LabelServiceID:            execTestServiceID,
			api.LabelServiceName:          testAppName,
			api.LabelDeploymentID:         "",
			api.LabelDeploymentGeneration: "2",
		},
	}, execTestServiceID)

	// An interrupted running start-first replace plan persisted after the new container became healthy.
	interrupted := &PersistedPlan{
		ID:          "cccccccccccccccccccccccccccccccc",
		ServiceID:   execTestServiceID,
		ServiceName: testAppName,
		Generation:  2,
		Spec:        spec,
		Status:      planStatusRunning,
		Steps: []*PersistedStep{{
			Kind:             stepReplaceContainer,
			Status:           stepRunning,
			MachineID:        execTestMachineID,
			MachineName:      execTestMachineName,
			Spec:             spec,
			Order:            api.UpdateOrderStartFirst,
			ContainerID:      oldID,
			NewContainerID:   newID,
			NewContainerName: testAppNewName,
			Phase:            phaseHealthy,
		}},
	}
	cli.storePlan(interrupted)
	cli.generations[execTestServiceID] = interrupted.Generation

	// The client reruns. The freshly planned operation must be ignored in favour of resuming the stored plan.
	sp := &ServicePlan{
		ServiceID:   execTestServiceID,
		ServiceName: testAppName,
		Spec:        spec,
		SequenceOperation: operation.SequenceOperation{Operations: []operation.Operation{
			&operation.ReplaceContainerOperation{
				ServiceID:    execTestServiceID,
				Spec:         spec,
				MachineID:    execTestMachineID,
				MachineName:  execTestMachineName,
				OldContainer: cli.containers[oldID].toServiceContainer(),
				Order:        api.UpdateOrderStartFirst,
			},
		}},
	}

	require.NoError(t, runServicePlan(t, cli, sp))

	// No extra container was created; the old one was stopped and removed and the plan completed.
	assert.NotContains(t, cli.calls, "create:service")
	assert.Contains(t, cli.calls, "stop:"+shortFakeID(oldID))
	assert.Contains(t, cli.calls, "remove:"+shortFakeID(oldID))
	assert.Contains(t, cli.containers, newID)
	assert.NotContains(t, cli.containers, oldID)

	plan := cli.storedPlan(t, execTestServiceID)
	assert.Equal(t, planStatusCompleted, plan.Status)
	assert.Equal(t, interrupted.ID, plan.ID)
}

func TestExecutorAdoptsContainerAfterLostCreateResponse(t *testing.T) {
	t.Parallel()

	cli := newFakeExecClient()
	cli.lostCreate = true // the create response is lost but the container now exists in Docker
	spec := execTestSpec("nginx:1")

	sp := &ServicePlan{
		ServiceID:   execTestServiceID,
		ServiceName: testAppName,
		Spec:        spec,
		SequenceOperation: operation.SequenceOperation{Operations: []operation.Operation{
			&operation.RunContainerOperation{
				ServiceID:   execTestServiceID,
				Spec:        spec,
				MachineID:   execTestMachineID,
				MachineName: execTestMachineName,
			},
		}},
	}

	require.NoError(t, runServicePlan(t, cli, sp))

	// Only one create call happened and exactly one container exists: the orphaned creation was adopted.
	var creates int
	for _, call := range cli.calls {
		if call == "create:service" {
			creates++
		}
	}
	assert.Equal(t, 1, creates)
	assert.Len(t, cli.containers, 1)
}

func TestExecutorPreDeployHookRunsOnce(t *testing.T) {
	t.Parallel()

	spec := execTestSpec("nginx:1")
	spec.PreDeploy = &api.PreDeployHook{Command: []string{"migrate"}}
	spec = spec.SetDefaults()

	cli := newFakeExecClient()
	hookID := "dddddddddddddddddddddddddddddddd"
	// The hook already exited successfully before the client disconnected.
	cli.record(&fakeContainer{
		id: hookID, name: testHookName, running: false, status: containerStatusExited, exitCode: 0,
		labels: map[string]string{
			api.LabelServiceID:   execTestServiceID,
			api.LabelServiceName: testAppName,
			api.LabelHook:        api.LabelHookPreDeploy,
		},
	}, execTestServiceID)

	interrupted := &PersistedPlan{
		ID: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", ServiceID: execTestServiceID, ServiceName: testAppName,
		Generation: 1, Spec: spec, Status: planStatusRunning,
		Steps: []*PersistedStep{{
			Kind: stepRunPreDeploy, Status: stepRunning,
			MachineID: execTestMachineID, MachineName: execTestMachineName,
			Spec: spec, NewContainerID: hookID, NewContainerName: testHookName, Phase: phaseStarted,
		}},
	}
	cli.storePlan(interrupted)
	cli.generations[execTestServiceID] = interrupted.Generation

	sp := &ServicePlan{ServiceID: execTestServiceID, ServiceName: testAppName, Spec: spec}
	require.NoError(t, runServicePlan(t, cli, sp))

	assert.NotContains(t, cli.calls, "create:hook")
	plan := cli.storedPlan(t, execTestServiceID)
	assert.Equal(t, planStatusCompleted, plan.Status)
}

func TestExecutorStopsWhenSuperseded(t *testing.T) {
	t.Parallel()

	cli := newFakeExecClient()
	cli.genOverride = 7 // a newer deployment/scaling/stop/removal already advanced the generation
	spec := execTestSpec("nginx:1")

	sp := &ServicePlan{
		ServiceID:   execTestServiceID,
		ServiceName: testAppName,
		Spec:        spec,
		SequenceOperation: operation.SequenceOperation{Operations: []operation.Operation{
			&operation.RunContainerOperation{
				ServiceID: execTestServiceID, Spec: spec,
				MachineID: execTestMachineID, MachineName: execTestMachineName,
			},
		}},
	}

	err := runServicePlan(t, cli, sp)
	require.ErrorIs(t, err, ErrPlanSuperseded)
	assert.NotContains(t, cli.calls, "create:service")
	assert.Empty(t, cli.containers)
}

func TestExecutorRefusesToRemoveNewerContainer(t *testing.T) {
	t.Parallel()

	cli := newFakeExecClient()
	spec := execTestSpec("nginx:2")

	newID := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	oldID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cli.record(&fakeContainer{
		id: newID, name: testAppNewName, running: true, status: containerStatusRunning,
		labels: map[string]string{
			api.LabelServiceID:            execTestServiceID,
			api.LabelServiceName:          testAppName,
			api.LabelDeploymentGeneration: "2",
		},
	}, execTestServiceID)
	// The old container was replaced by an even newer deployment (generation 5) while the plan was interrupted.
	cli.record(&fakeContainer{
		id: oldID, name: testAppOldName, running: true, status: containerStatusRunning,
		labels: map[string]string{
			api.LabelServiceID:            execTestServiceID,
			api.LabelServiceName:          testAppName,
			api.LabelDeploymentGeneration: "5",
		},
	}, execTestServiceID)

	interrupted := &PersistedPlan{
		ID: "cccccccccccccccccccccccccccccccc", ServiceID: execTestServiceID, ServiceName: testAppName,
		Generation: 2, Spec: spec, Status: planStatusRunning,
		Steps: []*PersistedStep{{
			Kind: stepReplaceContainer, Status: stepRunning,
			MachineID: execTestMachineID, MachineName: execTestMachineName,
			Spec: spec, Order: api.UpdateOrderStartFirst, ContainerID: oldID,
			NewContainerID: newID, NewContainerName: testAppNewName, Phase: phaseHealthy,
		}},
	}
	cli.storePlan(interrupted)
	cli.generations[execTestServiceID] = interrupted.Generation

	sp := &ServicePlan{ServiceID: execTestServiceID, ServiceName: testAppName, Spec: spec}
	err := runServicePlan(t, cli, sp)
	require.ErrorIs(t, err, ErrPlanSuperseded)

	// The newer container is still running; the plan did not stop or remove it.
	assert.Contains(t, cli.containers, oldID)
	assert.NotContains(t, cli.calls, "remove:"+shortFakeID(oldID))
	plan := cli.storedPlan(t, execTestServiceID)
	assert.Equal(t, planStatusSuperseded, plan.Status)
}

func TestExecutorFencedRunningPlanIsSupersededAndRedeployed(t *testing.T) {
	t.Parallel()

	cli := newFakeExecClient()
	spec := execTestSpec("nginx:1")

	stale := &PersistedPlan{
		ID: "cccccccccccccccccccccccccccccccc", ServiceID: execTestServiceID, ServiceName: testAppName,
		Generation: 1, Spec: spec, Status: planStatusRunning,
		Steps: []*PersistedStep{{
			Kind: stepRunContainer, Status: stepRunning,
			MachineID: execTestMachineID, MachineName: execTestMachineName, Spec: spec,
		}},
	}
	cli.storePlan(stale)
	// A service stop/removal fenced the generation while the stale plan was interrupted.
	cli.generations[execTestServiceID] = 2

	sp := &ServicePlan{
		ServiceID:   execTestServiceID,
		ServiceName: testAppName,
		Spec:        spec,
		SequenceOperation: operation.SequenceOperation{Operations: []operation.Operation{
			&operation.RunContainerOperation{
				ServiceID: execTestServiceID, Spec: spec,
				MachineID: execTestMachineID, MachineName: execTestMachineName,
			},
		}},
	}

	// The rerun succeeds: the stale plan is superseded and the fresh plan completes the deployment with exactly
	// one container creation (the stale plan's step is never executed).
	require.NoError(t, runServicePlan(t, cli, sp))
	var creates int
	for _, call := range cli.calls {
		if call == "create:service" {
			creates++
		}
	}
	assert.Equal(t, 1, creates)

	var statuses []PlanStatus
	for _, data := range cli.plans[execTestServiceID] {
		plan, err := decodePlan(data)
		require.NoError(t, err)
		statuses = append(statuses, plan.Status)
	}
	assert.ElementsMatch(t, []PlanStatus{planStatusSuperseded, planStatusCompleted}, statuses)
}

func TestExecutorNoOpPlanPersistsNothing(t *testing.T) {
	t.Parallel()

	cli := newFakeExecClient()
	spec := execTestSpec("nginx:1")
	sp := &ServicePlan{
		ServiceID:   execTestServiceID,
		ServiceName: testAppName,
		Spec:        spec,
	}

	require.NoError(t, runServicePlan(t, cli, sp))
	assert.Empty(t, cli.plans[execTestServiceID])
	gen, err := cli.GetServiceDeploymentGeneration(context.Background(), execTestServiceID)
	require.NoError(t, err)
	assert.Zero(t, gen)
}

func TestExecutorDifferentSpecSupersedesInterruptedPlan(t *testing.T) {
	t.Parallel()

	cli := newFakeExecClient()
	oldSpec := execTestSpec("nginx:1")
	newSpec := execTestSpec("nginx:2")

	interrupted := &PersistedPlan{
		ID: "cccccccccccccccccccccccccccccccc", ServiceID: execTestServiceID, ServiceName: testAppName,
		Generation: 3, Spec: oldSpec, Status: planStatusRunning,
		Steps: []*PersistedStep{{
			Kind: stepRunContainer, Status: stepPending,
			MachineID: execTestMachineID, MachineName: execTestMachineName, Spec: oldSpec,
		}},
	}
	cli.storePlan(interrupted)
	cli.generations[execTestServiceID] = 3

	sp := &ServicePlan{
		ServiceID:   execTestServiceID,
		ServiceName: testAppName,
		Spec:        newSpec,
		SequenceOperation: operation.SequenceOperation{Operations: []operation.Operation{
			&operation.RunContainerOperation{
				ServiceID: execTestServiceID, Spec: newSpec,
				MachineID: execTestMachineID, MachineName: execTestMachineName,
			},
		}},
	}

	require.NoError(t, runServicePlan(t, cli, sp))

	// The old plan is marked superseded and a new generation-4 plan completes the new deployment.
	var statuses []PlanStatus
	for _, data := range cli.plans[execTestServiceID] {
		plan, err := decodePlan(data)
		require.NoError(t, err)
		statuses = append(statuses, plan.Status)
	}
	assert.ElementsMatch(t, []PlanStatus{planStatusSuperseded, planStatusCompleted}, statuses)

	gen, err := cli.GetServiceDeploymentGeneration(context.Background(), execTestServiceID)
	require.NoError(t, err)
	assert.EqualValues(t, 4, gen)
}
