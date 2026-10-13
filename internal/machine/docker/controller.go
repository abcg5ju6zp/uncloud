package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/psviderski/uncloud/internal/machine/store"
	"github.com/psviderski/uncloud/pkg/api"
)

const (
	NetworkName = "uncloud"
	// maxSnapshotSettlePasses bounds how many times the initial snapshot reconciliation is repeated while Docker
	// events keep arriving in the quiet window. The final reconciliation is always a full, Docker-authoritative one.
	maxSnapshotSettlePasses = 100
)

// Default timing for the controller. Per-controller fields allow tests to shorten the lifecycle without
// mutating package globals.
var (
	// EventsDebounceInterval defines how long to wait before processing the next Docker event. Multiple events
	// occurring within this window are processed together as a single reconciliation to prevent system overload.
	EventsDebounceInterval = 100 * time.Millisecond
	// SyncInterval defines a regular interval to fully reconcile containers to the cluster store as a fallback
	// for missed events and to heartbeat the trusted container generation.
	SyncInterval = 30 * time.Second
)

// containerStore is the subset of the cluster store used by the Controller. *store.Store implements it; tests
// provide fakes.
type containerStore interface {
	ListContainers(ctx context.Context, opts store.ListOptions) ([]store.ContainerRecord, error)
	ReconcileMachineContainers(
		ctx context.Context, machineID string, containers []api.ServiceContainer, deleteIDs []string,
	) error
	GetContainerGeneration(ctx context.Context, machineID string) (store.ContainerGeneration, bool, error)
	SetContainerGeneration(ctx context.Context, gen store.ContainerGeneration) error
	MarkContainersOutdated(ctx context.Context, machineID string) error
}

// Controller monitors Docker events and synchronises service containers with the cluster store. It maintains a
// per-machine, monotonically numbered inventory generation. A generation is trusted only after a Docker event
// subscription is established, a complete snapshot (regular service containers and one-off deployment hooks)
// is applied to the store transactionally, and all events that arrived in between have been reconciled as well.
//
// Whenever Docker cannot be enumerated, the event stream breaks, or a store update fails, the machine's container
// facts are published as untrusted and its records are marked outdated. DNS, Caddy and deployments on every
// cluster machine then stop using them until a fresh full reconciliation confirms a new generation. This prevents
// stale containers from being selected as replicas after a Docker daemon restart or a socket repair.
type Controller struct {
	// machineID is the ID of the machine where the managed Docker daemon is running.
	machineID string
	client    *client.Client
	service   *Service
	store     containerStore

	// listContainers returns the complete managed container inventory from the local Docker daemon, including
	// regular service containers and one-off deployment hooks. Tests override it.
	listContainers func(ctx context.Context) (ListServiceContainersResult, error)

	// eventsDebounceInterval and syncInterval control reconciliation timing and default to the package
	// variables of the same names.
	eventsDebounceInterval time.Duration
	syncInterval           time.Duration

	mu      sync.Mutex
	current generationState
	// pendingReuse makes the next beginGeneration reuse current.Generation instead of bumping it. Set when the
	// generation was invalidated before the watch loop ever confirmed it (daemon start, pre-subscriber failures).
	pendingReuse bool
}

// generationState is the in-process view of the current inventory generation, shared by the watch loop, the
// periodic sync and any other caller of the controller.
type generationState struct {
	id      uint64
	trusted bool
	reason  string
}

func (c *Controller) currentGeneration() generationState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *Controller) isCurrentGeneration(gen uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current.id == gen
}

// syncError classifies a generation synchronisation failure so the generation row can be published with the
// right untrusted reason.
type syncError struct {
	reason string
	err    error
	// reuseGeneration indicates the next generation should keep the same number instead of bumping it, because
	// the failure happened before this generation established an event subscription or was ever trusted.
	reuseGeneration bool
}

func (e *syncError) Error() string { return e.err.Error() }
func (e *syncError) Unwrap() error { return e.err }

func NewController(machineID string, service *Service, st *store.Store) *Controller {
	c := &Controller{
		machineID:              machineID,
		client:                 service.Client,
		service:                service,
		store:                  st,
		eventsDebounceInterval: EventsDebounceInterval,
		syncInterval:           SyncInterval,
	}
	c.listContainers = func(ctx context.Context) (ListServiceContainersResult, error) {
		// List containers for all services, including stopped ones and deployment hooks.
		return service.ListServiceContainers(ctx, "", container.ListOptions{All: true})
	}
	return c
}

// WaitDaemonReady waits for the Docker daemon to start and be ready to serve requests.
func (c *Controller) WaitDaemonReady(ctx context.Context) error {
	if _, err := c.client.Ping(ctx); err == nil {
		return nil
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	ready, waitingLogged := false, false
	for !ready {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_, err := c.client.Ping(ctx)
			if err == nil {
				ready = true
				break
			}
			if !client.IsErrConnectionFailed(err) {
				return fmt.Errorf("connect to Docker daemon: %w", err)
			}
			if !waitingLogged {
				slog.Info("Waiting for Docker daemon to start and be ready.")
				waitingLogged = true
			}
		}
	}
	return nil
}

// InvalidateGeneration immediately bumps (or starts) the machine's container generation to an untrusted state
// and marks the machine's container records outdated. It is called when uncloudd starts, before store consumers
// (DNS, Caddy) are started, so pre-restart records are never treated as confirmed replicas before the daemon
// re-establishes an event subscription and a full snapshot. It is safe to call again later; the watch loop owns
// subsequent generation transitions.
func (c *Controller) InvalidateGeneration(ctx context.Context) error {
	if _, err := c.startGeneration(ctx, true); err != nil {
		return fmt.Errorf("invalidate container generation: %w", err)
	}
	// The watcher's first generation must reuse the number invalidated here instead of bumping it.
	c.mu.Lock()
	c.pendingReuse = true
	c.mu.Unlock()
	return nil
}

// WatchAndSyncContainers supervises inventory generations until the context is cancelled. Each generation
// subscribes to Docker events, takes a full snapshot and reconciles continuously. Failures invalidate the
// generation and a new one begins after a backoff, so a restarted Docker daemon is always followed by a fresh
// resubscription and full reconciliation.
func (c *Controller) WatchAndSyncContainers(ctx context.Context) error {
	boff := backoff.WithContext(backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(100*time.Millisecond),
		backoff.WithMaxInterval(30*time.Second),
		backoff.WithMaxElapsedTime(0),
	), ctx)

	for ctx.Err() == nil {
		err := c.runGeneration(ctx, boff)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			c.handleGenerationFailure(ctx, err)
			slog.Error("Container inventory generation failed, starting a new one.", "err", err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(boff.NextBackOff()):
		}
	}
	return nil
}

// handleGenerationFailure publishes the failed generation as untrusted with its reason and marks the machine's
// records outdated. Both writes are best effort: they will be re-attempted by the next generation.
func (c *Controller) handleGenerationFailure(ctx context.Context, err error) {
	var se *syncError
	if !errors.As(err, &se) {
		return
	}

	state := c.currentGeneration()
	c.mu.Lock()
	c.current = generationState{id: state.id, trusted: false, reason: se.reason}
	c.pendingReuse = se.reuseGeneration
	c.mu.Unlock()

	if state.id == 0 {
		// beginGeneration failed before establishing a generation number; it retries from scratch.
		return
	}
	if uErr := c.store.MarkContainersOutdated(ctx, c.machineID); uErr != nil {
		slog.Error("Failed to mark containers outdated after generation failure.", "err", uErr)
	}
	if uErr := c.store.SetContainerGeneration(ctx, store.ContainerGeneration{
		MachineID:  c.machineID,
		Generation: state.id,
		Trusted:    false,
		Reason:     se.reason,
	}); uErr != nil {
		slog.Error("Failed to publish untrusted container generation.", "err", uErr)
	}
}

// beginGeneration starts the watcher's next generation, honouring a pending reuse set by
// InvalidateGeneration or a pre-trust failure.
func (c *Controller) beginGeneration(ctx context.Context) (uint64, error) {
	c.mu.Lock()
	reuse := c.pendingReuse
	c.pendingReuse = false
	c.mu.Unlock()

	return c.startGeneration(ctx, reuse)
}

// startGeneration starts an untrusted generation: it marks existing records outdated and publishes the
// untrusted generation before any Docker interaction, so consumers can never mix this invalidation with a
// stale trusted state. When reuse is true and an in-process generation number exists, that number is kept.
func (c *Controller) startGeneration(ctx context.Context, reuse bool) (uint64, error) {
	c.mu.Lock()
	prev := c.current
	c.mu.Unlock()

	var gen uint64
	switch {
	case reuse && prev.id > 0:
		// Generation was already invalidated in this process (e.g. InvalidateGeneration) and never confirmed.
		gen = prev.id
	case prev.id > 0:
		gen = prev.id + 1
	default:
		// First generation of this process (e.g. after an uncloudd restart). Read the replicated previous
		// generation to keep numbers monotonic across restarts.
		g, ok, err := c.store.GetContainerGeneration(ctx, c.machineID)
		if err != nil {
			return 0, &syncError{
				reason:          store.GenerationReasonStoreUpdateFailed,
				err:             fmt.Errorf("get current container generation: %w", err),
				reuseGeneration: true,
			}
		}
		if ok {
			gen = g.Generation + 1
		} else {
			gen = 1
		}
	}

	// Mark the records outdated first: consumers filtering on sync_status drop them even before the untrusted
	// generation row replicates.
	if err := c.store.MarkContainersOutdated(ctx, c.machineID); err != nil {
		return 0, &syncError{
			reason:          store.GenerationReasonStoreUpdateFailed,
			err:             fmt.Errorf("mark containers outdated: %w", err),
			reuseGeneration: true,
		}
	}
	if err := c.store.SetContainerGeneration(ctx, store.ContainerGeneration{
		MachineID:  c.machineID,
		Generation: gen,
		Trusted:    false,
		Reason:     store.GenerationReasonSyncing,
	}); err != nil {
		return 0, &syncError{
			reason:          store.GenerationReasonStoreUpdateFailed,
			err:             fmt.Errorf("publish untrusted generation: %w", err),
			reuseGeneration: true,
		}
	}

	c.mu.Lock()
	c.current = generationState{id: gen, trusted: false, reason: store.GenerationReasonSyncing}
	c.mu.Unlock()
	slog.Debug("Started a new untrusted container inventory generation.", "generation", gen)

	return gen, nil
}

// runGeneration runs a single generation to completion: invalidation, event subscription, snapshot with a quiet
// barrier, trust publication and steady-state watching. It returns when the generation can no longer be trusted
// or when the context is cancelled.
func (c *Controller) runGeneration(ctx context.Context, boff backoff.BackOff) error {
	gen, bErr := c.beginGeneration(ctx)
	if bErr != nil {
		return bErr
	}

	if err := c.WaitDaemonReady(ctx); err != nil {
		return &syncError{
			reason:          store.GenerationReasonDockerUnreachable,
			err:             err,
			reuseGeneration: true,
		}
	}

	// A generation-scoped context cancels the event subscription as soon as the generation ends, fencing off
	// every event and callback belonging to it.
	genCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Filter only local container events. Subscribe BEFORE taking the snapshot so no event in between is missed.
	eventOpts := events.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("scope", "local"),
			filters.Arg("type", string(events.ContainerEventType)),
		),
	}
	eventCh, errCh := c.client.Events(genCtx, eventOpts)
	slog.Debug("Taking a full container snapshot for a new inventory generation.", "generation", gen)

	if err := c.snapshotAndSettle(genCtx, gen, eventCh, errCh); err != nil {
		return err
	}

	// Snapshot and all intervening events are reflected in the store. Mark the generation trusted.
	if err := c.publishTrusted(genCtx, gen); err != nil {
		return &syncError{reason: store.GenerationReasonStoreUpdateFailed, err: err}
	}
	if boff != nil {
		boff.Reset()
	}
	slog.Info("Container inventory generation confirmed as trusted.", "generation", gen)

	return c.watchGeneration(genCtx, gen, eventCh, errCh)
}

// snapshotAndSettle repeatedly performs a full Docker-authoritative reconciliation until no inventory-relevant
// event arrives within a quiet window following a successful reconciliation. This guarantees that both the
// snapshot and the events that arrived while taking it are synchronised before the generation is trusted.
func (c *Controller) snapshotAndSettle(
	ctx context.Context, gen uint64, eventCh <-chan events.Message, errCh <-chan error,
) error {
	quiet := time.NewTimer(c.eventsDebounceInterval)
	defer quiet.Stop()

	for pass := 0; pass < maxSnapshotSettlePasses; pass++ {
		if err := c.reconcile(ctx, gen); err != nil {
			return err
		}

		quiet.Reset(c.eventsDebounceInterval)
	settle:
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-quiet.C:
				// No inventory event during the quiet window: the last reconciliation reflects Docker as of now.
				return nil
			case e, ok := <-eventCh:
				if !ok {
					return c.streamError(errCh)
				}
				if !isInventoryEvent(e.Action) {
					// Events that cannot change the inventory (e.g. exec_create) must not delay trust.
					continue
				}
				slog.Debug("Docker event arrived while settling the container snapshot, reconciling again.",
					"generation", gen, "container_id", e.Actor.ID, "action", e.Action)
				// Coalesce any further pending events into this pass.
				drainInventoryEvents(eventCh)
				break settle
			case err := <-errCh:
				if ctx.Err() != nil {
					return nil
				}
				return &syncError{
					reason: store.GenerationReasonEventStreamBroken,
					err:    fmt.Errorf("receive Docker event during snapshot: %w", err),
				}
			}
		}
	}

	// Event bursts never really last this long, but do not block trust forever. One final full reconciliation
	// leaves the store matching Docker as closely as possible.
	if err := c.reconcile(ctx, gen); err != nil {
		return err
	}
	slog.Warn("Container snapshot settle kept seeing events; trusting generation after the maximum passes.",
		"generation", gen, "passes", maxSnapshotSettlePasses)
	return nil
}

// watchGeneration is the steady state for a trusted generation: debounced event-triggered reconciliations,
// periodic full reconciliations with a trust heartbeat, and generation invalidation when Docker or the event
// stream fail.
func (c *Controller) watchGeneration(
	ctx context.Context, gen uint64, eventCh <-chan events.Message, errCh <-chan error,
) error {
	ticker := time.NewTicker(c.syncInterval)
	defer ticker.Stop()

	trigger := make(chan events.Message, 1)
	var debouncer *time.Timer
	armDebouncer := func(e events.Message) {
		if debouncer == nil {
			debouncer = time.AfterFunc(c.eventsDebounceInterval, func() {
				select {
				case trigger <- e:
				default:
				}
			})
		} else {
			debouncer.Reset(c.eventsDebounceInterval)
		}
	}

	for {
		select {
		case <-ctx.Done():
			if debouncer != nil {
				debouncer.Stop()
			}
			return nil
		case e := <-eventCh:
			if isInventoryEvent(e.Action) {
				armDebouncer(e)
			}
		case e := <-trigger:
			if !c.isCurrentGeneration(gen) {
				// Fence off events scheduled by a previous generation: they must never overwrite a newer snapshot.
				slog.Debug("Ignoring Docker event from an outdated container generation.",
					"generation", gen, "container_id", e.Actor.ID)
				continue
			}
			slog.Debug("Reconciling containers to cluster store, triggered by a Docker container event.",
				"generation", gen, "container_id", e.Actor.ID, "action", e.Action)
			if err := c.reconcile(ctx, gen); err != nil {
				return err
			}
		case <-ticker.C:
			slog.Debug("Reconciling containers to cluster store, triggered by a regular interval.",
				"generation", gen, "interval", c.syncInterval)
			if err := c.reconcile(ctx, gen); err != nil {
				return err
			}
			// Refresh the trust heartbeat so other machines keep accepting this generation's containers.
			c.heartbeat(ctx, gen)
		case err, ok := <-errCh:
			if ctx.Err() != nil {
				return nil
			}
			if !ok {
				err = errors.New("docker event stream closed")
			}
			return &syncError{
				reason: store.GenerationReasonEventStreamBroken,
				err:    fmt.Errorf("receive Docker event: %w", err),
			}
		}
	}
}

// reconcile performs one full, Docker-authoritative reconciliation of the machine's store records, applying
// deletions and upserts in a single transaction. Store rows that Docker no longer reports are deleted by their
// Docker container ID, reported containers are inserted or updated, and the failure of any statement rolls the
// whole transaction back so the inventory is never replaced only partially.
func (c *Controller) reconcile(ctx context.Context, gen uint64) error {
	if !c.isCurrentGeneration(gen) {
		// Never let an old generation write over a newer snapshot.
		return &syncError{
			reason: store.GenerationReasonEventStreamBroken,
			err:    fmt.Errorf("stale container generation %d, a newer generation is active", gen),
		}
	}

	result, err := c.listContainers(ctx)
	if err != nil {
		return &syncError{
			reason: store.GenerationReasonDockerUnreachable,
			err:    fmt.Errorf("list service containers: %w", err),
		}
	}
	// Both regular and one-off hook containers make the complete snapshot.
	current := make([]api.ServiceContainer, 0, len(result.Containers)+len(result.HookContainers))
	current = append(current, result.Containers...)
	current = append(current, result.HookContainers...)

	// Include outdated/untrusted records so reconciliation converges to Docker reality by container identity.
	records, err := c.store.ListContainers(ctx, store.ListOptions{
		MachineIDs:         []string{c.machineID},
		IncludeUnconfirmed: true,
	})
	if err != nil {
		return &syncError{
			reason: store.GenerationReasonStoreUpdateFailed,
			err:    fmt.Errorf("list containers from store: %w", err),
		}
	}

	deleteIDs, upserts := planReconciliation(records, current)
	if err = c.store.ReconcileMachineContainers(ctx, c.machineID, upserts, deleteIDs); err != nil {
		return &syncError{reason: store.GenerationReasonStoreUpdateFailed, err: err}
	}
	return nil
}

// planReconciliation computes which store records must be deleted (Docker no longer reports them by container
// ID) and which Docker containers must be upserted (all of them; the store skips unchanged rows).
func planReconciliation(
	records []store.ContainerRecord, current []api.ServiceContainer,
) (deleteIDs []string, upserts []api.ServiceContainer) {
	currentIDs := make(map[string]struct{}, len(current))
	for _, ctr := range current {
		currentIDs[ctr.ID] = struct{}{}
		upserts = append(upserts, ctr)
	}
	for _, r := range records {
		if _, ok := currentIDs[r.Container.ID]; !ok {
			deleteIDs = append(deleteIDs, r.Container.ID)
		}
	}
	return deleteIDs, upserts
}

func (c *Controller) publishTrusted(ctx context.Context, gen uint64) error {
	if err := c.store.SetContainerGeneration(ctx, store.ContainerGeneration{
		MachineID:  c.machineID,
		Generation: gen,
		Trusted:    true,
	}); err != nil {
		return fmt.Errorf("publish trusted generation: %w", err)
	}
	c.mu.Lock()
	c.current = generationState{id: gen, trusted: true}
	c.mu.Unlock()
	return nil
}

// heartbeat refreshes updated_at of a still-current trusted generation without changing its number.
func (c *Controller) heartbeat(ctx context.Context, gen uint64) {
	state := c.currentGeneration()
	if state.id != gen || !state.trusted {
		return
	}
	if err := c.store.SetContainerGeneration(ctx, store.ContainerGeneration{
		MachineID:  c.machineID,
		Generation: gen,
		Trusted:    true,
	}); err != nil {
		slog.Error("Failed to heartbeat trusted container generation.", "generation", gen, "err", err)
	}
}

func (c *Controller) streamError(errCh <-chan error) error {
	select {
	case err, ok := <-errCh:
		if !ok || err == nil {
			return &syncError{
				reason: store.GenerationReasonEventStreamBroken,
				err:    errors.New("docker event stream closed"),
			}
		}
		return &syncError{
			reason: store.GenerationReasonEventStreamBroken,
			err:    fmt.Errorf("docker event stream closed: %w", err),
		}
	default:
		return &syncError{
			reason: store.GenerationReasonEventStreamBroken,
			err:    errors.New("docker event stream closed"),
		}
	}
}

// drainInventoryEvents removes currently buffered inventory events so the next full reconciliation
// coalesces them into one pass.
func drainInventoryEvents(ch <-chan events.Message) {
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

// isInventoryEvent reports whether an action may create, remove or change a container and therefore requires
// a store reconciliation.
func isInventoryEvent(action events.Action) bool {
	switch action {
	case events.ActionCreate,
		events.ActionStart,
		events.ActionStop,
		events.ActionPause,
		events.ActionUnPause,
		events.ActionKill,
		events.ActionDie,
		events.ActionOOM,
		events.ActionDestroy,
		events.ActionHealthStatusRunning,
		events.ActionHealthStatusHealthy,
		events.ActionHealthStatusUnhealthy:
		return true
	default:
		return false
	}
}
