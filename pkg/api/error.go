package api

import (
	"errors"
	"fmt"
	"strings"
)

var ErrNotFound = errors.New("not found")

// MachineInventoryStatus identifies a machine whose container inventory could not be confirmed during a
// service query: the machine is not UP/SUSPECT, it did not answer the inventory broadcast, or its Docker
// daemon failed to enumerate containers.
type MachineInventoryStatus struct {
	MachineID   string
	MachineName string
	// Reason explains why the inventory for this machine is unavailable or unconfirmed.
	Reason string
}

// ErrIncompleteContainerInventory means a service query could not collect the container inventory from
// every relevant machine. The service may exist with no containers on reachable machines (hence it wraps
// ErrNotFound), but deployments, replica counting and cleanup must not proceed from the partial result.
type ErrIncompleteContainerInventory struct {
	UnavailableMachines []MachineInventoryStatus
}

// NewErrIncompleteContainerInventory builds the inventory error from unavailable machines.
func NewErrIncompleteContainerInventory(unavailable []MachineInventoryStatus) *ErrIncompleteContainerInventory {
	return &ErrIncompleteContainerInventory{UnavailableMachines: unavailable}
}

func (e *ErrIncompleteContainerInventory) Error() string {
	var names []string
	for _, m := range e.UnavailableMachines {
		name := m.MachineName
		if name == "" {
			name = m.MachineID
		}
		if m.Reason != "" {
			name += " (" + m.Reason + ")"
		}
		names = append(names, name)
	}
	return fmt.Sprintf("container inventory cannot be confirmed on machine(s): %s", strings.Join(names, ", "))
}

// Unwrap returns ErrNotFound because an incomplete inventory cannot prove whether the service exists.
func (e *ErrIncompleteContainerInventory) Unwrap() error { return ErrNotFound }
