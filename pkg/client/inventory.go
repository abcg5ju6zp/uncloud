package client

import (
	"fmt"

	"github.com/psviderski/uncloud/api/pb"
	"github.com/psviderski/uncloud/internal/cli/tui"
	"github.com/psviderski/uncloud/internal/machine/docker"
	"github.com/psviderski/uncloud/pkg/api"
)

// UnavailableInventoryMachines compares the queried cluster machines with their broadcast answers and
// returns the machines that could not confirm their container inventory: machines that are not UP or
// SUSPECT (not queried), queried machines that never answered, and machines whose Docker daemon failed
// to enumerate containers. Deployments and replica counting must fail closed when this list is non-empty
// instead of treating missing containers as "not running there".
func UnavailableInventoryMachines(
	machines api.MachineMembersList, responses []docker.MachineServiceContainers,
) []api.MachineInventoryStatus {
	answered := make(map[string]bool)
	var answeredError map[string]api.MachineInventoryStatus
	var unavailable []api.MachineInventoryStatus

	for _, mc := range responses {
		if mc.Metadata == nil || mc.Metadata.MachineId == "" {
			continue
		}
		answered[mc.Metadata.MachineId] = true
		if mc.Metadata.Error != "" {
			if answeredError == nil {
				answeredError = make(map[string]api.MachineInventoryStatus)
			}
			answeredError[mc.Metadata.MachineId] = api.MachineInventoryStatus{
				MachineID:   mc.Metadata.MachineId,
				MachineName: mc.Metadata.MachineName,
				Reason:      mc.Metadata.Error,
			}
		}
	}

	seen := make(map[string]bool)
	add := func(s api.MachineInventoryStatus) {
		if s.MachineID == "" || seen[s.MachineID] {
			return
		}
		seen[s.MachineID] = true
		unavailable = append(unavailable, s)
	}

	for _, m := range machines {
		if m.Machine == nil {
			continue
		}
		switch m.State {
		case pb.MachineMember_UP, pb.MachineMember_SUSPECT:
			// The machine was queried. Its answer may still be missing or carry an error.
		default:
			add(api.MachineInventoryStatus{
				MachineID:   m.Machine.Id,
				MachineName: m.Machine.Name,
				Reason:      fmt.Sprintf("machine state is %s", m.State.String()),
			})
			continue
		}

		if errStatus, ok := answeredError[m.Machine.Id]; ok {
			add(errStatus)
			continue
		}
		if !answered[m.Machine.Id] {
			add(api.MachineInventoryStatus{
				MachineID:   m.Machine.Id,
				MachineName: m.Machine.Name,
				Reason:      "machine did not respond to the container inventory request",
			})
		}
	}

	return unavailable
}

// warnUnavailableInventory explicitly warns the user about machines whose inventory could not be
// confirmed, so service queries never silently present partial results as complete.
func warnUnavailableInventory(statuses []api.MachineInventoryStatus) {
	for _, s := range statuses {
		name := s.MachineName
		if name == "" {
			name = s.MachineID
		}
		tui.PrintWarning(fmt.Sprintf("cannot confirm container inventory on machine '%s': %s. "+
			"The results may be incomplete and deployments/cleanup are blocked until this machine recovers.",
			name, s.Reason))
	}
}
