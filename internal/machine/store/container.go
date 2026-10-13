package store

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/docker/docker/api/types/container"
	"github.com/psviderski/uncloud/internal/corrosion"
	"github.com/psviderski/uncloud/pkg/api"
)

const (
	// SyncStatusSynced indicates that a container record is synchronised with the Docker daemon. A record may
	// become outdated even when the status is "synced" if the machine crashes or a network partition occurs.
	// The machine's container generation (see container_generations table) is therefore also checked to
	// determine if the record can be trusted.
	SyncStatusSynced = "synced"
	// SyncStatusOutdated indicates that a container record may be outdated, for example, due to being unable
	// to retrieve the container's state from the Docker daemon, a broken event stream, an incomplete store
	// update, or when the machine is being stopped or restarted.
	SyncStatusOutdated = "outdated"

	// containerChangesDebounceInterval defines how long to wait before notifying subscribers about container changes.
	// Multiple changes within this window are grouped into a single notification to prevent system overload.
	containerChangesDebounceInterval = 250 * time.Millisecond
)

type ContainerRecord struct {
	Container  api.ServiceContainer
	MachineID  string
	SyncStatus string
	UpdatedAt  time.Time
}

type ListOptions struct {
	// MachineIDs filters containers by the machine IDs they are running on.
	MachineIDs      []string
	ServiceIDOrName ServiceIDOrNameOptions
	// IncludeUnconfirmed returns records of machines whose current container generation is not trusted or
	// unknown, and records explicitly marked as outdated. Such records may not reflect Docker reality and
	// must not be used to select replicas (DNS, Caddy) or to plan deployments and cleanup. The machine
	// controller uses this option to reconcile its own records. Everyone else should keep it unset.
	IncludeUnconfirmed bool
}

// ServiceIDOrNameOptions filters containers by the service ID or name they are part of. If both ID and Name are
// provided, they are combined with an OR operator.
type ServiceIDOrNameOptions struct {
	ID   string
	Name string
}

type DeleteOptions struct {
	// IDs filters containers by their container IDs.
	IDs []string
	// MachineIDs filters containers by the machine IDs they are running on.
	MachineIDs []string
}

// freshnessModifier is the SQLite time modifier matching generations whose heartbeat is still fresh, e.g.
// datetime('now', '-90 seconds').
func freshnessModifier() string {
	return fmt.Sprintf("-%d seconds", int(GenerationFreshnessInterval.Seconds()))
}

// applyTrustGate restricts a containers SELECT to records that can be trusted: the record must be synced and
// the owning machine must publish a trusted container generation whose heartbeat is fresh. The latter drops
// stale records of machines that lost their Docker event stream, crashed, or are taking a new snapshot even
// before their untrusted row replicates to this machine.
func applyTrustGate(q sq.SelectBuilder) sq.SelectBuilder {
	return q.
		Join("container_generations g ON g.machine_id = c.machine_id").
		Where(sq.Eq{"c.sync_status": SyncStatusSynced}).
		Where("g.trusted = ? AND g.updated_at >= datetime('now', ?)", 1, freshnessModifier())
}

// ListContainers returns a list of container records from the store database that match the given options.
// The result excludes orphan containers whose machine is no longer in the cluster and, unless
// IncludeUnconfirmed is set, containers whose owning machine cannot currently confirm its inventory
// (untrusted, unknown, or stale container generation) or that are marked outdated.
func (s *Store) ListContainers(ctx context.Context, opts ListOptions) ([]ContainerRecord, error) {
	q := sq.Select("c.id", "c.container", "c.machine_id", "c.sync_status", "c.updated_at").
		From("containers c").
		Join("machines m ON m.id = c.machine_id")

	if !opts.IncludeUnconfirmed {
		q = applyTrustGate(q)
	}

	if len(opts.MachineIDs) > 0 {
		q = q.Where(sq.Eq{"c.machine_id": opts.MachineIDs})
	}

	if opts.ServiceIDOrName.ID != "" || opts.ServiceIDOrName.Name != "" {
		var conditions []sq.Sqlizer
		if opts.ServiceIDOrName.ID != "" {
			conditions = append(conditions, sq.Eq{"c.service_id": opts.ServiceIDOrName.ID})
		}
		if opts.ServiceIDOrName.Name != "" {
			conditions = append(conditions, sq.Eq{"c.service_name": opts.ServiceIDOrName.Name})
		}
		q = q.Where(sq.Or(conditions))
	}

	query, args, err := q.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rows, err := s.corro.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select query: %w", err)
	}
	defer rows.Close()

	var containers []ContainerRecord
	for rows.Next() {
		cr, err := scanContainerRecord(rows)
		if err != nil {
			return nil, err
		}
		if cr == nil {
			continue
		}
		containers = append(containers, *cr)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate container records: %w", err)
	}

	return containers, nil
}

// scanContainerRecord scans one containers row. It returns nil for a partially replicated row whose
// container JSON is still empty.
func scanContainerRecord(rows rowScanner) (*ContainerRecord, error) {
	var id, cJSON, machineID, syncStatus, updatedAtStr string
	if err := rows.Scan(&id, &cJSON, &machineID, &syncStatus, &updatedAtStr); err != nil {
		return nil, fmt.Errorf("scan container record: %w", err)
	}

	// Skip containers with empty JSON data. This can happen during partial replication
	// when cr-sqlite has created the row but the container column hasn't been synced yet.
	if cJSON == "" || cJSON == "{}" {
		slog.Debug("Skipping container with empty data in the store (partial replication?).", "id", id)
		return nil, nil
	}

	var c api.ServiceContainer
	if err := json.Unmarshal([]byte(cJSON), &c); err != nil {
		return nil, fmt.Errorf("unmarshal container: %w", err)
	}
	updatedAt, err := time.Parse(time.DateTime, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("parse updated_at: %w", err)
	}
	return &ContainerRecord{
		Container:  c,
		MachineID:  machineID,
		SyncStatus: syncStatus,
		UpdatedAt:  updatedAt,
	}, nil
}

// ReconcileMachineContainers makes the store rows of one machine match the containers reported by its
// Docker daemon. Records missing from dockerContainers are deleted, all reported containers (regular
// service containers and one-off deployment hooks) are inserted or updated, and surviving rows return
// to the synced status. All statements run in a single transaction so the inventory is never replaced
// only partially: a failure leaves the previous records untouched (and the caller keeps or restores
// the untrusted generation state).
func (s *Store) ReconcileMachineContainers(
	ctx context.Context, machineID string, dockerContainers []api.ServiceContainer, deleteIDs []string,
) error {
	var statements []corrosion.Statement

	if len(deleteIDs) > 0 {
		query, args, err := sq.Delete("containers").Where(sq.Eq{"id": deleteIDs}).ToSql()
		if err != nil {
			return fmt.Errorf("build delete query: %w", err)
		}
		statements = append(statements, corrosion.Statement{Query: query, Params: args})
	}

	for _, ctr := range dockerContainers {
		// Stabilise the order of slices that Docker returns non-deterministically, so that byte-level
		// comparison of the serialised container does not flag spurious changes.
		normaliseContainerForStore(&ctr)

		cJSON, err := json.Marshal(ctr)
		if err != nil {
			return fmt.Errorf("marshal container: %w", err)
		}

		statements = append(statements, corrosion.Statement{
			Query: `
				INSERT INTO containers (id, container, machine_id, sync_status, updated_at)
				VALUES (?, ?, ?, ?, datetime('now'))
				ON CONFLICT (id) DO UPDATE SET container   = excluded.container,
				                               machine_id  = excluded.machine_id,
				                               sync_status = excluded.sync_status,
				                               updated_at  = excluded.updated_at
				WHERE containers.container != excluded.container
				   OR containers.machine_id != excluded.machine_id
				   OR containers.sync_status != excluded.sync_status`,
			Params: []any{ctr.ID, string(cJSON), machineID, SyncStatusSynced},
		})
	}

	if len(statements) == 0 {
		return nil
	}

	if _, err := s.corro.ExecMultiContext(ctx, statements...); err != nil {
		return fmt.Errorf("reconcile containers for machine '%s': %w", machineID, err)
	}
	if len(dockerContainers) > 0 || len(deleteIDs) > 0 {
		slog.Debug("Reconciled machine containers in store DB.",
			"machine_id", machineID, "containers", len(dockerContainers), "deleted", len(deleteIDs))
	}
	return nil
}

// MarkContainersOutdated marks all currently synced container records of a machine as outdated. It is
// used when the machine can no longer vouch for its inventory (Docker unreachable, event stream broken,
// failed or partial store update), so consumers filtering on the synced status stop selecting them even
// before the new generation becomes trusted.
func (s *Store) MarkContainersOutdated(ctx context.Context, machineID string) error {
	if _, err := s.corro.ExecContext(ctx, `
		UPDATE containers
		SET sync_status = ?, updated_at = datetime('now')
		WHERE machine_id = ? AND sync_status != ?`,
		SyncStatusOutdated, machineID, SyncStatusOutdated); err != nil {
		return fmt.Errorf("mark containers outdated for machine '%s': %w", machineID, err)
	}
	return nil
}

// normaliseContainerForStore removes potentially sensitive data and normalises the container fields that Docker may
// return in non-deterministic order so that byte-level comparison of the serialised container does not flag spurious
// changes.
func normaliseContainerForStore(ctr *api.ServiceContainer) {
	// Remove the environment variables to avoid leaking secrets.
	ctr.Config.Env = nil
	ctr.ServiceSpec.Container.Env = nil
	if ctr.ServiceSpec.PreDeploy != nil {
		ctr.ServiceSpec.PreDeploy.Env = nil
	}

	// Docker returns Mounts in a non-deterministic order so sort them.
	slices.SortFunc(ctr.Mounts, func(a, b container.MountPoint) int {
		return cmp.Or(
			strings.Compare(a.Destination, b.Destination),
			strings.Compare(a.Source, b.Source),
		)
	})
}

// DeleteContainers deletes container records from the store database that match the given options.
// If no filter is set, all container records are deleted. Filters are combined with AND.
func (s *Store) DeleteContainers(ctx context.Context, opts DeleteOptions) error {
	q := sq.Delete("containers")
	if len(opts.IDs) > 0 {
		q = q.Where(sq.Eq{"id": opts.IDs})
	}
	if len(opts.MachineIDs) > 0 {
		q = q.Where(sq.Eq{"machine_id": opts.MachineIDs})
	}

	query, args, err := q.ToSql()
	if err != nil {
		return fmt.Errorf("build query: %w", err)
	}

	res, err := s.corro.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("delete query: %w", err)
	}
	if res.RowsAffected > 0 {
		slog.Debug("Container records deleted from store DB.",
			"ids", opts.IDs, "machine_ids", opts.MachineIDs, "count", res.RowsAffected)
	}

	return nil
}

// SubscribeContainers returns a list of containers and a channel that signals changes to the list. The channel doesn't
// receive any values, it just signals when a container(s) has been added, updated, or deleted in the database.
// The result excludes orphan containers whose machine is no longer in the cluster as well as containers of machines
// whose container generation is currently untrusted, unknown or stale (see applyTrustGate).
// The channel is closed when the containers are no longer subscribable: either the provided context is cancelled or
// the underlying subscription fails.
func (s *Store) SubscribeContainers(ctx context.Context) ([]ContainerRecord, <-chan struct{}, error) {
	q := sq.Select("c.id", "c.container", "c.machine_id", "c.sync_status", "c.updated_at").
		From("containers c").
		Join("machines m ON m.id = c.machine_id")
	q = applyTrustGate(q)

	query, args, err := q.ToSql()
	if err != nil {
		return nil, nil, fmt.Errorf("build query: %w", err)
	}

	sub, err := s.corro.SubscribeContext(ctx, query, args, false)
	if err != nil {
		return nil, nil, err
	}

	var containers []ContainerRecord
	rows := sub.Rows()
	for rows.Next() {
		cr, err := scanContainerRecord(rows)
		if err != nil {
			return nil, nil, err
		}
		if cr != nil {
			containers = append(containers, *cr)
		}
	}
	if err = rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate subscribed container records: %w", err)
	}

	events, err := sub.Changes()
	if err != nil {
		return nil, nil, fmt.Errorf("get subscription changes: %w", err)
	}

	changes := make(chan struct{})
	go func() {
		defer close(changes)
		// Coalesce bursts of rapid-fire row-level change events (e.g. from frequent health-check updates
		// across many services) into a single signal per debounce window, instead of forwarding one signal
		// per event. Every subscriber of this channel (e.g. the Caddy and DNS reconcilers) otherwise reruns
		// its full reconciliation on every single event, which can burn significant CPU across the cluster
		// when there's a lot of container churn.
		var debouncer *time.Timer
		var debouncerCh <-chan time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-events:
				if !ok {
					// events channel has been closed.
					if sub.Err() != nil {
						slog.Error("Containers subscription failed.", "id", sub.ID(), "err", sub.Err())
					}
					return
				}
				if debouncerCh == nil {
					if debouncer == nil {
						debouncer = time.NewTimer(containerChangesDebounceInterval)
					} else {
						debouncer.Reset(containerChangesDebounceInterval)
					}
					debouncerCh = debouncer.C
				}
			case <-debouncerCh:
				select {
				case changes <- struct{}{}:
				case <-ctx.Done():
					return
				}
				debouncerCh = nil
			}
		}
	}()

	return containers, changes, nil
}
