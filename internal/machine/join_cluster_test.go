package machine

import (
	"context"
	"net/netip"
	"testing"

	"github.com/psviderski/uncloud/api/pb"
	"github.com/psviderski/uncloud/internal/machine/network"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mustParsePrefix(t *testing.T, s string) netip.Prefix {
	t.Helper()
	return netip.MustParsePrefix(s)
}

func mustParseAddr(t *testing.T, s string) netip.Addr {
	t.Helper()
	return netip.MustParseAddr(s)
}

func mustParseAddrPort(t *testing.T, s string) netip.AddrPort {
	t.Helper()
	return netip.MustParseAddrPort(s)
}

func newTestNetworkConfig(t *testing.T) *network.Config {
	t.Helper()
	privKey, pubKey, err := network.NewMachineKeys()
	require.NoError(t, err)
	return &network.Config{PrivateKey: privKey, PublicKey: pubKey}
}

const (
	testJoinMachineID  = "machine-1"
	testJoinClusterID  = "cluster-1"
	testJoinID         = "join-1"
	testJoinGeneration = 42
)

func TestJoinCluster_IdempotentReplay(t *testing.T) {
	t.Parallel()

	newInitialisedMachine := func() *Machine {
		return &Machine{state: &State{
			ID:         testJoinMachineID,
			Name:       "web",
			ClusterID:  testJoinClusterID,
			JoinID:     testJoinID,
			Generation: testJoinGeneration,
		}}
	}

	t.Run("matching identity replays as success", func(t *testing.T) {
		m := newInitialisedMachine()
		req := &pb.JoinClusterRequest{
			ClusterId: testJoinClusterID,
			Machine: &pb.MachineInfo{
				Id:         testJoinMachineID,
				Generation: testJoinGeneration,
				JoinId:     testJoinID,
			},
		}
		resp, err := m.JoinCluster(context.Background(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("legacy request without generation replays by machine ID", func(t *testing.T) {
		m := newInitialisedMachine()
		req := &pb.JoinClusterRequest{
			Machine: &pb.MachineInfo{Id: testJoinMachineID},
		}
		_, err := m.JoinCluster(context.Background(), req)
		require.NoError(t, err)
	})

	t.Run("different generation is rejected", func(t *testing.T) {
		m := newInitialisedMachine()
		req := &pb.JoinClusterRequest{
			ClusterId: testJoinClusterID,
			Machine: &pb.MachineInfo{
				Id:         testJoinMachineID,
				Generation: testJoinGeneration + 1,
				JoinId:     testJoinID,
			},
		}
		_, err := m.JoinCluster(context.Background(), req)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("different join ID is rejected", func(t *testing.T) {
		m := newInitialisedMachine()
		req := &pb.JoinClusterRequest{
			ClusterId: testJoinClusterID,
			Machine: &pb.MachineInfo{
				Id:         testJoinMachineID,
				Generation: testJoinGeneration,
				JoinId:     "join-2",
			},
		}
		_, err := m.JoinCluster(context.Background(), req)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("different cluster ID is rejected", func(t *testing.T) {
		m := newInitialisedMachine()
		req := &pb.JoinClusterRequest{
			ClusterId: "cluster-2",
			Machine: &pb.MachineInfo{
				Id:         testJoinMachineID,
				Generation: testJoinGeneration,
				JoinId:     testJoinID,
			},
		}
		_, err := m.JoinCluster(context.Background(), req)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("different machine ID is rejected", func(t *testing.T) {
		m := newInitialisedMachine()
		req := &pb.JoinClusterRequest{
			ClusterId: testJoinClusterID,
			Machine: &pb.MachineInfo{
				Id:         "machine-2",
				Generation: testJoinGeneration,
				JoinId:     testJoinID,
			},
		}
		_, err := m.JoinCluster(context.Background(), req)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
}

func TestJoinCluster_GenerationAndJoinIDTogether(t *testing.T) {
	t.Parallel()

	// An uninitialised machine whose state has a local public key. Only the new-style consistency check
	// is exercised, before any state is mutated.
	m := &Machine{state: &State{
		Network: newTestNetworkConfig(t),
	}}

	network := &pb.NetworkConfig{
		Subnet:       pb.NewIPPrefix(mustParsePrefix(t, "10.210.5.0/24")),
		ManagementIp: pb.NewIP(mustParseAddr(t, "fdcc::5")),
		Endpoints:    []*pb.IPPort{pb.NewIPPort(mustParseAddrPort(t, "10.0.0.5:51820"))},
		PublicKey:    m.state.Network.PublicKey,
	}

	_, err := m.JoinCluster(context.Background(), &pb.JoinClusterRequest{
		ClusterId: "cluster-1",
		Machine: &pb.MachineInfo{
			Id:         "machine-9",
			Name:       "web",
			Generation: 42,
			JoinId:     "", // missing while generation is set
			Network:    network,
		},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.ErrorContains(t, err, "generation and join ID")
	require.Empty(t, m.state.ID, "state must not be mutated on invalid request")
}
