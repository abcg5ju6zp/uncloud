package client

import (
	"testing"

	"github.com/psviderski/uncloud/api/pb"
	"github.com/psviderski/uncloud/internal/machine/docker"
	"github.com/psviderski/uncloud/pkg/api"
	"github.com/stretchr/testify/assert"
)

const (
	machineOneName   = "one"
	machineTwoName   = "two"
	machineThreeName = "three"
	machineFourName  = "four"
	dockerDownError  = "rpc error: cannot connect to the Docker daemon"
)

func member(state pb.MachineMember_MembershipState, id, name string) *pb.MachineMember {
	return &pb.MachineMember{
		Machine: &pb.MachineInfo{Id: id, Name: name},
		State:   state,
	}
}

func TestUnavailableInventoryMachines(t *testing.T) {
	t.Parallel()

	t.Run("all machines answer without errors", func(t *testing.T) {
		t.Parallel()

		machines := api.MachineMembersList{
			member(pb.MachineMember_UP, "m1", machineOneName),
			member(pb.MachineMember_SUSPECT, "m2", machineTwoName),
		}
		responses := []docker.MachineServiceContainers{
			{Metadata: &pb.Metadata{MachineId: "m1", MachineName: machineOneName}},
			{Metadata: &pb.Metadata{MachineId: "m2", MachineName: machineTwoName}},
		}

		assert.Empty(t, UnavailableInventoryMachines(machines, responses))
	})

	t.Run("down, silent and erroring machines are reported", func(t *testing.T) {
		t.Parallel()

		machines := api.MachineMembersList{
			member(pb.MachineMember_UP, "m1", machineOneName),
			member(pb.MachineMember_SUSPECT, "m2", machineTwoName),
			member(pb.MachineMember_DOWN, "m3", machineThreeName),
			member(pb.MachineMember_UNKNOWN, "m4", machineFourName),
		}
		// m1 answers, m2 answers with a Docker enumeration error, m3/m4 were never queried.
		responses := []docker.MachineServiceContainers{
			{Metadata: &pb.Metadata{MachineId: "m1", MachineName: machineOneName}},
			{Metadata: &pb.Metadata{
				MachineId:   "m2",
				MachineName: machineTwoName,
				Error:       dockerDownError,
			}},
		}

		unavailable := UnavailableInventoryMachines(machines, responses)
		requireStatuses(t, unavailable, map[string]string{
			"m2": dockerDownError,
			"m3": "machine state is DOWN",
			"m4": "machine state is UNKNOWN",
		})
	})

	t.Run("queried machine that never answered is detected", func(t *testing.T) {
		t.Parallel()

		machines := api.MachineMembersList{
			member(pb.MachineMember_UP, "m1", machineOneName),
			member(pb.MachineMember_SUSPECT, "m2", machineTwoName),
		}
		responses := []docker.MachineServiceContainers{
			{Metadata: &pb.Metadata{MachineId: "m1", MachineName: machineOneName}},
		}

		requireStatuses(t, UnavailableInventoryMachines(machines, responses), map[string]string{
			"m2": "machine did not respond to the container inventory request",
		})
	})
}

func requireStatuses(t *testing.T, statuses []api.MachineInventoryStatus, want map[string]string) {
	t.Helper()

	got := make(map[string]string, len(statuses))
	for _, s := range statuses {
		got[s.MachineID] = s.Reason
	}
	assert.Len(t, statuses, len(want))
	for id, reason := range want {
		assert.Equal(t, reason, got[id], "machine %s", id)
	}
}

func TestErrIncompleteContainerInventoryWrapsNotFound(t *testing.T) {
	t.Parallel()

	err := api.NewErrIncompleteContainerInventory([]api.MachineInventoryStatus{
		{MachineID: "m1", MachineName: machineOneName, Reason: "machine state is DOWN"},
	})

	assert.ErrorIs(t, err, api.ErrNotFound)
	assert.Contains(t, err.Error(), machineOneName)
	assert.Contains(t, err.Error(), "machine state is DOWN")
}
