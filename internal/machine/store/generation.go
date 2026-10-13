package store

import (
	"context"
	"fmt"
	"time"
)

const (
	// GenerationReasonSyncing means a new generation has begun and the machine is taking a fresh
	// full snapshot of its Docker containers. Its previously synced records must not be trusted yet.
	GenerationReasonSyncing = "syncing"
	// GenerationReasonDockerUnreachable means the Docker daemon could not be reached or failed
	// to enumerate its containers, so the actual container inventory is unknown.
	GenerationReasonDockerUnreachable = "docker_unreachable"
	// GenerationReasonEventStreamBroken means the Docker event channel was closed or errored.
	// Events may have been missed, so the store records can no longer be assumed to match Docker.
	GenerationReasonEventStreamBroken = "event_stream_broken"
	// GenerationReasonStoreUpdateFailed means a snapshot or event synchronisation could not be
	// fully written to the distributed store, so the store may contain a partial inventory.
	GenerationReasonStoreUpdateFailed = "store_update_failed"
)

// GenerationFreshnessInterval bounds how old a trusted generation heartbeat may be before its
// containers stop being selected despite the row still claiming to be trusted. The machine
// controller heartbeats a trusted generation on every periodic full reconciliation (well within
// this interval). It lets consumers stop trusting a machine that crashed or was partitioned
// before it could publish an untrusted generation.
const GenerationFreshnessInterval = 90 * time.Second

// ContainerGeneration is a machine's own assessment of whether its container records in the
// containers table can be trusted as a complete and current reflection of its Docker daemon.
type ContainerGeneration struct {
	MachineID  string
	Generation uint64
	// Trusted is true only after the event subscription, full snapshot and intervening events
	// have all been synchronised.
	Trusted bool
	// Reason explains why the generation is not trusted. Empty for a trusted generation.
	Reason    string
	UpdatedAt time.Time
}

// SetContainerGeneration publishes the machine's current generation state to the distributed
// store. Calling it with the same generation number and trusted value refreshes updated_at,
// which is used as the trust heartbeat.
func (s *Store) SetContainerGeneration(ctx context.Context, gen ContainerGeneration) error {
	if gen.MachineID == "" {
		return fmt.Errorf("machine ID cannot be empty")
	}

	trusted := 0
	if gen.Trusted {
		trusted = 1
	}

	if _, err := s.corro.ExecContext(ctx, `
		INSERT INTO container_generations (machine_id, generation, trusted, reason, updated_at)
		VALUES (?, ?, ?, ?, datetime('now'))
		ON CONFLICT (machine_id) DO UPDATE SET generation = excluded.generation,
		                                      trusted    = excluded.trusted,
		                                      reason     = excluded.reason,
		                                      updated_at = excluded.updated_at`,
		gen.MachineID, gen.Generation, trusted, gen.Reason); err != nil {
		return fmt.Errorf("upsert container generation for machine '%s': %w", gen.MachineID, err)
	}
	return nil
}

// DeleteContainerGeneration removes the generation row of a machine, e.g. when the machine is
// removed from the cluster.
func (s *Store) DeleteContainerGeneration(ctx context.Context, machineID string) error {
	if _, err := s.corro.ExecContext(ctx,
		"DELETE FROM container_generations WHERE machine_id = ?", machineID); err != nil {
		return fmt.Errorf("delete container generation for machine '%s': %w", machineID, err)
	}
	return nil
}

// GetContainerGeneration returns the generation published by the given machine. The boolean is
// false when the machine has never published one.
func (s *Store) GetContainerGeneration(
	ctx context.Context, machineID string,
) (ContainerGeneration, bool, error) {
	rows, err := s.corro.QueryContext(ctx, `
		SELECT machine_id, generation, trusted, reason, updated_at
		FROM container_generations
		WHERE machine_id = ?`, machineID)
	if err != nil {
		return ContainerGeneration{}, false, fmt.Errorf("query container generation: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return ContainerGeneration{}, false, fmt.Errorf("iterate container generation: %w", err)
		}
		return ContainerGeneration{}, false, nil
	}

	gen, err := scanContainerGeneration(rows)
	if err != nil {
		return ContainerGeneration{}, false, err
	}
	return gen, true, nil
}

// ListContainerGenerations returns the generation state of every machine that has published one,
// keyed by machine ID.
func (s *Store) ListContainerGenerations(ctx context.Context) (map[string]ContainerGeneration, error) {
	rows, err := s.corro.QueryContext(ctx, `
		SELECT machine_id, generation, trusted, reason, updated_at
		FROM container_generations`)
	if err != nil {
		return nil, fmt.Errorf("query container generations: %w", err)
	}
	defer rows.Close()

	generations := make(map[string]ContainerGeneration)
	for rows.Next() {
		gen, err := scanContainerGeneration(rows)
		if err != nil {
			return nil, err
		}
		generations[gen.MachineID] = gen
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate container generations: %w", err)
	}
	return generations, nil
}

// rowScanner is satisfied by corrosion.Rows and lets generation scans be shared by callers.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanContainerGeneration(rows rowScanner) (ContainerGeneration, error) {
	var (
		gen          ContainerGeneration
		machineID    string
		reason       string
		updatedAtStr string
		trusted      int
	)
	if err := rows.Scan(&machineID, &gen.Generation, &trusted, &reason, &updatedAtStr); err != nil {
		return ContainerGeneration{}, fmt.Errorf("scan container generation: %w", err)
	}
	updatedAt, err := time.Parse(time.DateTime, updatedAtStr)
	if err != nil {
		return ContainerGeneration{}, fmt.Errorf("parse generation updated_at: %w", err)
	}
	gen.MachineID = machineID
	gen.Trusted = trusted == 1
	gen.Reason = reason
	gen.UpdatedAt = updatedAt
	return gen, nil
}
