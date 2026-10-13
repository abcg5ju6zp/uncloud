package cluster

import (
	"net/netip"
	"testing"

	"github.com/psviderski/uncloud/api/pb"
	"github.com/psviderski/uncloud/internal/machine/network"
	"github.com/psviderski/uncloud/pkg/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testMachineInfo(id, name string, generation uint64, joinID string, state pb.JoinState) *pb.MachineInfo {
	// WireGuard keys are 32 bytes.
	pubKey := make([]byte, 32)
	copy(pubKey, "pk-"+id)
	subnet := netip.MustParsePrefix("10.210.0.0/24")
	return &pb.MachineInfo{
		Id:         id,
		Name:       name,
		Generation: generation,
		JoinId:     joinID,
		JoinState:  state,
		Network: &pb.NetworkConfig{
			Subnet:       pb.NewIPPrefix(subnet),
			ManagementIp: pb.NewIP(network.ManagementIP(pubKey)),
			PublicKey:    pubKey,
			Endpoints:    []*pb.IPPort{pb.NewIPPort(netip.MustParseAddrPort("1.2.3.4:51820"))},
		},
	}
}

func TestNextMemberGeneration(t *testing.T) {
	t.Parallel()

	// With no existing members, a positive generation is allocated.
	first := nextMemberGeneration(nil)
	assert.NotZero(t, first)

	// Many sequential allocations stay unique and increasing.
	existing := []*pb.MachineInfo{}
	prev := uint64(0)
	for range 1000 {
		gen := nextMemberGeneration(existing)
		assert.Greater(t, gen, prev)
		existing = append(existing, &pb.MachineInfo{Generation: gen})
		prev = gen
	}

	// A huge observed generation (e.g. a concurrent member with a future clock) is strictly advanced.
	future := []*pb.MachineInfo{{Generation: 1 << 62}}
	gen := nextMemberGeneration(future)
	assert.Greater(t, gen, uint64(1<<62))
}

func TestPickRegistrationLoser(t *testing.T) {
	t.Parallel()

	old := testMachineInfo("a", "web", 100, "join-a", pb.JoinState_JOIN_STATE_PENDING)
	newer := testMachineInfo("b", "web", 200, "join-b", pb.JoinState_JOIN_STATE_PENDING)

	// Between two PENDING registrations the older generation wins, so the newer one is revoked.
	assert.Equal(t, newer, pickRegistrationLoser(old, newer))
	assert.Equal(t, newer, pickRegistrationLoser(newer, old))

	// An ACTIVE membership always beats a PENDING newcomer.
	active := testMachineInfo("c", "web", 300, "join-c", pb.JoinState_JOIN_STATE_ACTIVE)
	assert.Equal(t, newer, pickRegistrationLoser(newer, active))
	assert.Equal(t, newer, pickRegistrationLoser(active, newer))

	// Same generation is deterministically broken by join ID.
	sameGen1 := testMachineInfo("a", "web", 100, "aaa", pb.JoinState_JOIN_STATE_PENDING)
	sameGen2 := testMachineInfo("b", "web", 100, "bbb", pb.JoinState_JOIN_STATE_PENDING)
	assert.Equal(t, sameGen2, pickRegistrationLoser(sameGen1, sameGen2))
	assert.Equal(t, sameGen2, pickRegistrationLoser(sameGen2, sameGen1))
}

func TestFindConflictingMember(t *testing.T) {
	t.Parallel()

	registered := testMachineInfo("new", "web", 100, "join-new", pb.JoinState_JOIN_STATE_PENDING)

	t.Run("same name", func(t *testing.T) {
		other := testMachineInfo("old", "web", 50, "join-old", pb.JoinState_JOIN_STATE_PENDING)
		other.Network.PublicKey = []byte("different-key")
		found, field := findConflictingMember(registered, []*pb.MachineInfo{registered, other})
		require.NotNil(t, found)
		assert.Equal(t, "old", found.Id)
		assert.Equal(t, "name", field)
	})

	t.Run("same public key", func(t *testing.T) {
		other := testMachineInfo("old", "db", 50, "join-old", pb.JoinState_JOIN_STATE_PENDING)
		other.Network.PublicKey = registered.Network.PublicKey
		found, field := findConflictingMember(registered, []*pb.MachineInfo{other, registered})
		require.NotNil(t, found)
		assert.Equal(t, "old", found.Id)
		assert.Equal(t, "public key", field)
	})

	t.Run("no conflict", func(t *testing.T) {
		other := testMachineInfo("old", "db", 50, "join-old", pb.JoinState_JOIN_STATE_PENDING)
		other.Network.PublicKey = []byte("different-key")
		found, _ := findConflictingMember(registered, []*pb.MachineInfo{registered, other})
		assert.Nil(t, found)
	})

	t.Run("ignores itself", func(t *testing.T) {
		found, _ := findConflictingMember(registered, []*pb.MachineInfo{registered})
		assert.Nil(t, found)
	})
}

func TestIsPendingJoin(t *testing.T) {
	t.Parallel()

	assert.True(t, api.IsPendingJoin(testMachineInfo("a", "web", 1, "j", pb.JoinState_JOIN_STATE_PENDING)))
	assert.False(t, api.IsPendingJoin(testMachineInfo("b", "web", 1, "j", pb.JoinState_JOIN_STATE_ACTIVE)))
	// Legacy memberships without a join state are treated as active.
	assert.False(t, api.IsPendingJoin(testMachineInfo("c", "web", 0, "", pb.JoinState_JOIN_STATE_UNSPECIFIED)))
}
