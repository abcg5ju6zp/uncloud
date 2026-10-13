package cli

import (
	"net/netip"
	"path/filepath"
	"testing"

	"github.com/psviderski/uncloud/api/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustParseAddrPort(t *testing.T, s string) netip.AddrPort {
	t.Helper()
	return netip.MustParseAddrPort(s)
}

func mustParsePrefixV4(t *testing.T, s string) netip.Prefix {
	t.Helper()
	return netip.MustParsePrefix(s)
}

func mustParseAddrV6(t *testing.T, s string) netip.Addr {
	t.Helper()
	return netip.MustParseAddr(s)
}

const testJoinMachineID = "machine-1"

func newTestJoin(t *testing.T, joinID, contextName, publicKey string) *machineJoin {
	t.Helper()
	return &machineJoin{
		JoinID:      joinID,
		ContextName: contextName,
		Remote: remoteJoinHost{
			User: rootUser,
			Host: "10.0.0.1",
			Port: 22,
		},
		PublicKey:     []byte(publicKey),
		Endpoints:     []*pb.IPPort{pb.NewIPPort(mustParseAddrPort(t, "10.0.0.1:51820"))},
		Name:          "web",
		WireguardPort: 51820,
	}
}

func TestJoinStore_SaveListFind(t *testing.T) {
	t.Parallel()

	store := newJoinStore(filepath.Join(t.TempDir(), "config.yaml"))

	// No joins yet.
	joins, err := store.list()
	require.NoError(t, err)
	assert.Empty(t, joins)

	join := newTestJoin(t, "join-1", "default", "pk-1")
	require.NoError(t, store.save(join, joinPhasePrepared))

	// Saving again advances the phase and keeps a single file.
	require.NoError(t, store.save(join, joinPhaseRegistered))
	join.Machine = &pb.MachineInfo{
		Id:         testJoinMachineID,
		Name:       "web",
		Generation: 42,
		JoinId:     "join-1",
		ClusterId:  "cluster-1",
		Network: &pb.NetworkConfig{
			Subnet:       pb.NewIPPrefix(mustParsePrefixV4(t, "10.210.3.0/24")),
			ManagementIp: pb.NewIP(mustParseAddrV6(t, "fdcc::3")),
			PublicKey:    []byte("pk-1"),
			Endpoints:    []*pb.IPPort{pb.NewIPPort(mustParseAddrPort(t, "10.0.0.1:51820"))},
		},
	}
	require.NoError(t, store.save(join, joinPhaseRegistered))

	joins, err = store.list()
	require.NoError(t, err)
	require.Len(t, joins, 1)
	assert.Equal(t, joinPhaseRegistered, joins[0].Phase)
	assert.Equal(t, "join-1", joins[0].JoinID)

	// The registered membership with its subnet and generation must survive a JSON roundtrip.
	require.NotNil(t, joins[0].Machine)
	assert.Equal(t, testJoinMachineID, joins[0].Machine.Id)
	assert.Equal(t, uint64(42), joins[0].Machine.Generation)
	var subnet netip.Prefix
	subnet, err = joins[0].Machine.Network.Subnet.ToPrefix()
	require.NoError(t, err)
	assert.Equal(t, "10.210.3.0/24", subnet.String())

	// findResumable matches context and public key.
	found := store.findResumable("default", []byte("pk-1"))
	require.NotNil(t, found)
	assert.Equal(t, "join-1", found.JoinID)

	assert.Nil(t, store.findResumable("other-context", []byte("pk-1")))
	assert.Nil(t, store.findResumable("default", []byte("pk-other")))
}

func TestJoinStore_FindResumableSkipsRecorded(t *testing.T) {
	t.Parallel()

	store := newJoinStore(filepath.Join(t.TempDir(), "config.yaml"))
	join := newTestJoin(t, "join-1", "default", "pk-1")
	require.NoError(t, store.save(join, joinPhaseRecorded))

	assert.Nil(t, store.findResumable("default", []byte("pk-1")))
}

func TestJoinStore_Remove(t *testing.T) {
	t.Parallel()

	store := newJoinStore(filepath.Join(t.TempDir(), "config.yaml"))
	join := newTestJoin(t, "join-1", "default", "pk-1")
	require.NoError(t, store.save(join, joinPhasePrepared))

	// Removing a missing file is not an error.
	require.NoError(t, store.remove("does-not-exist"))

	require.NoError(t, store.remove("join-1"))
	joins, err := store.list()
	require.NoError(t, err)
	assert.Empty(t, joins)
}

func TestJoinStore_RemoveForMachine(t *testing.T) {
	t.Parallel()

	store := newJoinStore(filepath.Join(t.TempDir(), "config.yaml"))

	join1 := newTestJoin(t, "join-1", "default", "pk-1")
	join1.Machine = &pb.MachineInfo{Id: testJoinMachineID}
	require.NoError(t, store.save(join1, joinPhaseRegistered))

	join2 := newTestJoin(t, "join-2", "default", "pk-2")
	join2.Machine = &pb.MachineInfo{Id: "machine-2"}
	require.NoError(t, store.save(join2, joinPhaseRegistered))

	require.NoError(t, store.removeForMachine("default", testJoinMachineID, nil))

	joins, err := store.list()
	require.NoError(t, err)
	require.Len(t, joins, 1)
	assert.Equal(t, "join-2", joins[0].JoinID)

	// Matching by public key also removes.
	require.NoError(t, store.removeForMachine("default", "", []byte("pk-2")))
	joins, err = store.list()
	require.NoError(t, err)
	assert.Empty(t, joins)
}

func TestMachineJoin_AddRequest(t *testing.T) {
	t.Parallel()

	join := newTestJoin(t, "join-1", "default", "pk-1")
	join.PublicIP = &pb.IP{Ip: []byte{1, 2, 3, 4}}

	req := join.addRequest()
	assert.Equal(t, "join-1", req.JoinId)
	assert.Equal(t, "web", req.Name)
	assert.Equal(t, []byte("pk-1"), req.Network.PublicKey)
	require.Len(t, req.Network.Endpoints, 1)
	assert.Equal(t, uint32(51820), req.Network.Endpoints[0].Port)
	assert.Equal(t, []byte{1, 2, 3, 4}, req.PublicIp.Ip)
}

func TestMachineJoin_JoinRequest(t *testing.T) {
	t.Parallel()

	join := newTestJoin(t, "join-1", "default", "pk-1")
	join.Machine = &pb.MachineInfo{
		Id:         testJoinMachineID,
		Name:       "web",
		Generation: 42,
		JoinId:     "join-1",
		ClusterId:  "cluster-1",
	}
	join.ClusterID = "cluster-1"
	join.OtherMachines = []*pb.MachineInfo{{Id: "machine-0"}}
	join.MinStoreVersion = map[string]uint64{"actor-1": 7}

	req := join.joinRequest()
	assert.Equal(t, testJoinMachineID, req.Machine.Id)
	assert.Equal(t, "cluster-1", req.ClusterId)
	require.Len(t, req.OtherMachines, 1)
	assert.Equal(t, uint64(7), req.MinStoreVersion["actor-1"])
	assert.Equal(t, int32(51820), req.WireguardPort)
}
