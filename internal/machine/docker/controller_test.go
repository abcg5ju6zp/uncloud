package docker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	dockerclient "github.com/docker/docker/client"
	"github.com/psviderski/uncloud/internal/machine/store"
	"github.com/psviderski/uncloud/pkg/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeContainerStore is an in-memory implementation of the controller's containerStore interface.
type fakeContainerStore struct {
	mu      sync.Mutex
	gen     *store.ContainerGeneration
	records map[string]fakeRecord
}

type fakeRecord struct {
	ctr       api.ServiceContainer
	machineID string
	status    string
}

func newFakeContainerStore() *fakeContainerStore {
	return &fakeContainerStore{records: make(map[string]fakeRecord)}
}

func (f *fakeContainerStore) ListContainers(_ context.Context, opts store.ListOptions) ([]store.ContainerRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []store.ContainerRecord
	for _, r := range f.records {
		if len(opts.MachineIDs) > 0 {
			found := false
			for _, id := range opts.MachineIDs {
				if id == r.machineID {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		out = append(out, store.ContainerRecord{
			Container:  r.ctr,
			MachineID:  r.machineID,
			SyncStatus: r.status,
		})
	}
	return out, nil
}

func (f *fakeContainerStore) ReconcileMachineContainers(
	_ context.Context, machineID string, containers []api.ServiceContainer, deleteIDs []string,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, id := range deleteIDs {
		delete(f.records, id)
	}
	for _, ctr := range containers {
		normaliseForFake(ctr)
		f.records[ctr.ID] = fakeRecord{ctr: ctr, machineID: machineID, status: store.SyncStatusSynced}
	}
	return nil
}

func normaliseForFake(ctr api.ServiceContainer) {
	// The real store strips env before serialising; mirror it so comparisons are stable.
	ctr.Config.Env = nil
}

func (f *fakeContainerStore) GetContainerGeneration(
	_ context.Context, machineID string,
) (store.ContainerGeneration, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.gen == nil || f.gen.MachineID != machineID {
		return store.ContainerGeneration{}, false, nil
	}
	return *f.gen, true, nil
}

func (f *fakeContainerStore) SetContainerGeneration(_ context.Context, gen store.ContainerGeneration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	g := gen
	f.gen = &g
	return nil
}

func (f *fakeContainerStore) MarkContainersOutdated(_ context.Context, machineID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, r := range f.records {
		if r.machineID == machineID {
			r.status = store.SyncStatusOutdated
			f.records[id] = r
		}
	}
	return nil
}

func (f *fakeContainerStore) snapshot() (map[string]fakeRecord, *store.ContainerGeneration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	recs := make(map[string]fakeRecord, len(f.records))
	for k, v := range f.records {
		recs[k] = v
	}
	return recs, f.gen
}

func (f *fakeContainerStore) waitTrusted(t *testing.T, gen uint64) store.ContainerGeneration {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, g := f.snapshot()
		if g != nil && g.Trusted && g.Generation == gen {
			return *g
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, g := f.snapshot()
	if g == nil {
		t.Fatalf("generation %d not trusted after timeout (no generation published)", gen)
	}
	t.Fatalf("generation %d not trusted after timeout, got: %+v", gen, g)
	return store.ContainerGeneration{}
}

// dockerEventServer serves /_ping and /events. Each /events request stays open until breakAll is closed
// or that request's own context is done. breakOnce closes the first event stream to simulate a daemon
// restart.
type dockerEventServer struct {
	*httptest.Server

	mu         sync.Mutex
	subs       int
	breakOnce  chan struct{}
	firstClose sync.Once
}

func newDockerEventServer() *dockerEventServer {
	breakOnce := make(chan struct{})
	s := &dockerEventServer{breakOnce: breakOnce}

	mux := http.NewServeMux()
	ping := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("API-Version", "1.48")
		_, _ = w.Write([]byte("OK"))
	}
	mux.HandleFunc("/v1.48/_ping", ping)
	mux.HandleFunc("/_ping", ping)
	mux.HandleFunc("/v1.48/events", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.subs++
		n := s.subs
		s.mu.Unlock()

		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}

		if n == 1 {
			// First subscription breaks when breakOnce is closed to simulate a Docker daemon restart.
			select {
			case <-breakOnce:
			case <-r.Context().Done():
			}
			return
		}
		// Subsequent subscriptions stay open until the request is cancelled.
		<-r.Context().Done()
	})

	s.Server = httptest.NewServer(mux)
	return s
}

func newTestController(t *testing.T, machineID string, st *fakeContainerStore) (*Controller, *dockerEventServer) {
	t.Helper()

	srv := newDockerEventServer()
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	dockerClient, err := dockerclient.NewClientWithOpts(
		dockerclient.WithHost("tcp://"+u.Host),
		dockerclient.WithHTTPClient(srv.Client()),
		dockerclient.WithVersion("1.48"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dockerClient.Close() })

	svc := NewService(dockerClient, nil)
	ctrl := NewController(machineID, svc, nil)
	ctrl.store = st
	return ctrl, srv
}

func testServiceContainer(id string, hook bool) api.ServiceContainer {
	labels := map[string]string{
		api.LabelManaged:     "true",
		api.LabelServiceID:   "svc-id",
		api.LabelServiceName: "web",
	}
	if hook {
		labels[api.LabelHook] = api.LabelHookPreDeploy
	}
	return api.ServiceContainer{
		Container: api.Container{
			InspectResponse: container.InspectResponse{
				ContainerJSONBase: &container.ContainerJSONBase{
					ID:    id,
					State: &container.State{Running: true},
				},
				Config:          &container.Config{Labels: labels},
				NetworkSettings: &container.NetworkSettings{},
			},
		},
	}
}

const (
	testContainer1ID = "ctr-1"
	testContainer2ID = "ctr-2"
	testHook1ID      = "hook-1"
	testNew1ID       = "new-1"
)

func TestPlanReconciliation(t *testing.T) {
	t.Parallel()

	c1 := testServiceContainer(testContainer1ID, false)
	c2 := testServiceContainer(testContainer2ID, false)
	h1 := testServiceContainer(testHook1ID, true)

	records := []store.ContainerRecord{
		{Container: c1, MachineID: "m1", SyncStatus: store.SyncStatusSynced},
		{Container: c2, MachineID: "m1", SyncStatus: store.SyncStatusSynced},
	}

	t.Run("delete missing, upsert all current including hooks", func(t *testing.T) {
		t.Parallel()

		deleteIDs, upserts := planReconciliation(records, []api.ServiceContainer{c1, h1})
		assert.ElementsMatch(t, []string{testContainer2ID}, deleteIDs)
		assert.ElementsMatch(t, []string{testContainer1ID, testHook1ID},
			[]string{upserts[0].ID, upserts[1].ID})
	})

	t.Run("empty Docker inventory deletes every stale record", func(t *testing.T) {
		t.Parallel()

		deleteIDs, upserts := planReconciliation(records, nil)
		assert.ElementsMatch(t, []string{testContainer1ID, testContainer2ID}, deleteIDs)
		assert.Empty(t, upserts)
	})

	t.Run("new daemon identities are all upserted", func(t *testing.T) {
		t.Parallel()

		deleteIDs, upserts := planReconciliation(records, []api.ServiceContainer{
			testServiceContainer(testNew1ID, false),
		})
		assert.ElementsMatch(t, []string{testContainer1ID, testContainer2ID}, deleteIDs)
		assert.Equal(t, []string{testNew1ID}, []string{upserts[0].ID})
	})
}

// TestController_GenerationTrustedAfterSnapshot verifies the generation only becomes trusted once the
// event subscription and a full snapshot (regular containers plus hooks) are synchronised.
func TestController_GenerationTrustedAfterSnapshot(t *testing.T) {
	const machineID = "machine-1"
	st := newFakeContainerStore()
	ctrl, _ := newTestController(t, machineID, st)
	ctrl.eventsDebounceInterval = 5 * time.Millisecond
	ctrl.syncInterval = time.Hour

	regular := testServiceContainer("ctr-regular", false)
	hook := testServiceContainer("ctr-hook", true)
	ctrl.listContainers = func(context.Context) (ListServiceContainersResult, error) {
		return ListServiceContainersResult{Containers: []api.ServiceContainer{regular}, HookContainers: []api.ServiceContainer{hook}}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = ctrl.WatchAndSyncContainers(ctx) }()

	st.waitTrusted(t, 1)

	recs, gen := st.snapshot()
	require.NotNil(t, gen)
	assert.True(t, gen.Trusted)
	assert.Empty(t, gen.Reason)
	require.Len(t, recs, 2)
	assert.Equal(t, store.SyncStatusSynced, recs["ctr-regular"].status)
	assert.Equal(t, store.SyncStatusSynced, recs["ctr-hook"].status)
}

// TestController_UntrustedWhileDockerCannotEnumerate verifies the inventory stays uncertain while Docker
// listing fails and recovers with a full reconciliation afterwards.
func TestController_UntrustedWhileDockerCannotEnumerate(t *testing.T) {
	const machineID = "machine-1"
	st := newFakeContainerStore()
	ctrl, _ := newTestController(t, machineID, st)
	ctrl.eventsDebounceInterval = 5 * time.Millisecond
	ctrl.syncInterval = time.Hour

	// Seed a stale synced record as left over from a previous process.
	stale := testServiceContainer("ctr-stale", false)
	st.mu.Lock()
	st.records["ctr-stale"] = fakeRecord{ctr: stale, machineID: machineID, status: store.SyncStatusSynced}
	st.mu.Unlock()

	// Fail the first few enumeration attempts so at least one generation is forced to retry, then
	// Docker becomes available again with a different container identity.
	var attemptMu sync.Mutex
	failedAttempts := 0
	ctrl.listContainers = func(context.Context) (ListServiceContainersResult, error) {
		attemptMu.Lock()
		attempt := failedAttempts
		failedAttempts = attempt + 1
		attemptMu.Unlock()

		if attempt < 3 {
			return ListServiceContainersResult{}, fmt.Errorf("cannot connect to the Docker daemon")
		}
		return ListServiceContainersResult{Containers: []api.ServiceContainer{
			testServiceContainer("ctr-fresh", false),
		}}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = ctrl.WatchAndSyncContainers(ctx) }()

	// While enumeration fails, the stale record must be outdated and the generation untrusted.
	require.Eventually(t, func() bool {
		recs, gen := st.snapshot()
		if gen == nil || gen.Trusted {
			return false
		}
		if gen.Reason != store.GenerationReasonDockerUnreachable && gen.Reason != store.GenerationReasonSyncing {
			return false
		}
		r, ok := recs["ctr-stale"]
		return ok && r.status == store.SyncStatusOutdated
	}, 5*time.Second, 10*time.Millisecond, "stale containers must be marked outdated while Docker is unreachable")

	// Recovery lands on a later generation; wait for any trusted generation > 1.
	var trusted store.ContainerGeneration
	require.Eventually(t, func() bool {
		_, g := st.snapshot()
		if g != nil && g.Trusted && g.Generation > 1 {
			trusted = *g
			return true
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	assert.True(t, trusted.Trusted)

	recs, _ := st.snapshot()
	_, stalePresent := recs["ctr-stale"]
	assert.False(t, stalePresent, "the stale record must be reconciled away by Docker identity")
	require.Contains(t, recs, "ctr-fresh")
	assert.Equal(t, store.SyncStatusSynced, recs["ctr-fresh"].status)
}

// TestController_ResubscribeAfterStreamBreak verifies that a broken event stream invalidates the
// generation and a new subscription performs a full identity-based reconciliation, bumping the
// generation number.
func TestController_ResubscribeAfterStreamBreak(t *testing.T) {
	const machineID = "machine-1"
	st := newFakeContainerStore()
	ctrl, srv := newTestController(t, machineID, st)
	ctrl.eventsDebounceInterval = 5 * time.Millisecond
	ctrl.syncInterval = time.Hour

	oldCtr := testServiceContainer("ctr-old", false)
	newCtr := testServiceContainer("ctr-new", false)
	swapped := make(chan struct{})
	var swappedOnce sync.Once
	ctrl.listContainers = func(context.Context) (ListServiceContainersResult, error) {
		select {
		case <-swapped:
			return ListServiceContainersResult{Containers: []api.ServiceContainer{newCtr}}, nil
		default:
			return ListServiceContainersResult{Containers: []api.ServiceContainer{oldCtr}}, nil
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = ctrl.WatchAndSyncContainers(ctx) }()

	st.waitTrusted(t, 1)

	// Break the first event stream: Docker daemon restarted and the container identity changed.
	swappedOnce.Do(func() { close(swapped) })
	srv.firstClose.Do(func() { close(srv.breakOnce) })

	// A new generation must be confirmed and the store must converge to Docker's actual identity.
	st.waitTrusted(t, 2)
	recs, gen := st.snapshot()
	require.NotNil(t, gen)
	_, oldPresent := recs["ctr-old"]
	assert.False(t, oldPresent, "old container record must be deleted after the full re-reconciliation")
	require.Contains(t, recs, "ctr-new")
	assert.Equal(t, store.SyncStatusSynced, recs["ctr-new"].status)

	// More than one event subscription was established.
	require.Eventually(t, func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return srv.subs >= 2
	}, 5*time.Second, 10*time.Millisecond)
}

// TestController_InvalidateGenerationBeforeWatch verifies an explicit startup invalidation is reused
// by the watcher and marked trusted after the snapshot without bumping the generation number.
func TestController_InvalidateGenerationBeforeWatch(t *testing.T) {
	const machineID = "machine-1"
	st := newFakeContainerStore()
	ctrl, _ := newTestController(t, machineID, st)
	ctrl.eventsDebounceInterval = 5 * time.Millisecond
	ctrl.syncInterval = time.Hour
	ctr := testServiceContainer("ctr-1", false)
	ctrl.listContainers = func(context.Context) (ListServiceContainersResult, error) {
		return ListServiceContainersResult{Containers: []api.ServiceContainer{ctr}}, nil
	}

	require.NoError(t, ctrl.InvalidateGeneration(context.Background()))
	_, gen := st.snapshot()
	require.NotNil(t, gen)
	assert.False(t, gen.Trusted)
	assert.Equal(t, store.GenerationReasonSyncing, gen.Reason)
	assert.Equal(t, uint64(1), gen.Generation)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = ctrl.WatchAndSyncContainers(ctx) }()

	// The watcher may reuse gen 1 or bump to 2, but it eventually trusts and only generations >=1 exist.
	g := st.waitTrusted(t, 1)
	assert.Equal(t, uint64(1), g.Generation, "the invalidated generation should be reused")
}
