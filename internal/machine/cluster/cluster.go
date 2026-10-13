package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/netip"
	"slices"
	"time"

	"github.com/psviderski/uncloud/api/pb"
	"github.com/psviderski/uncloud/internal/corrosion"
	"github.com/psviderski/uncloud/internal/machine/network"
	"github.com/psviderski/uncloud/internal/machine/store"
	"github.com/psviderski/uncloud/internal/secret"
	"github.com/psviderski/uncloud/pkg/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	// clusterIDKey stores the unique cluster ID in the cluster key-value table.
	clusterIDKey = "cluster_id"
	// memberGenerationRandomBits is the number of low-order random bits in a membership generation.
	memberGenerationRandomBits = 20
	// concurrentRegistrationSettle is how long registration waits for a concurrent registration committed on
	// another cluster member to replicate before deciding it won the identity.
	concurrentRegistrationSettle = 500 * time.Millisecond
)

type Cluster struct {
	pb.UnimplementedClusterServer

	store      *store.Store
	corroAdmin *corrosion.AdminClient
	// machineID is the ID of the current machine that is running the cluster service.
	machineID string
	// initialised is closed when the machine is configured as a member of a cluster.
	initialised <-chan struct{}
	// ready is closed when the cluster controller has finished starting all components
	// and the machine is ready to serve cluster requests.
	ready <-chan struct{}
}

func NewCluster(store *store.Store, corroAdmin *corrosion.AdminClient, initialised, ready <-chan struct{}) *Cluster {
	return &Cluster{
		store:       store,
		corroAdmin:  corroAdmin,
		initialised: initialised,
		ready:       ready,
	}
}

// UpdateMachineID updates the current machine ID that is running the cluster service.
func (c *Cluster) UpdateMachineID(mid string) {
	c.machineID = mid
}

func (c *Cluster) Init(ctx context.Context, network netip.Prefix) error {
	select {
	case <-c.initialised:
		return fmt.Errorf("cluster is already initialised on this machine")
	default:
	}

	if err := c.store.Put(ctx, "network", network.String()); err != nil {
		return fmt.Errorf("put network to store: %w", err)
	}
	if err := c.store.Put(ctx, "created_at", time.Now().UTC().Format(time.RFC3339)); err != nil {
		return fmt.Errorf("put created_at to store: %w", err)
	}
	// Fix the cluster identity for its lifetime. Joins bind to it and can detect that the cluster was reset
	// and recreated.
	clusterID, err := secret.NewID()
	if err != nil {
		return fmt.Errorf("generate cluster ID: %w", err)
	}
	if err = c.store.Put(ctx, clusterIDKey, clusterID); err != nil {
		return fmt.Errorf("put cluster ID to store: %w", err)
	}
	return nil
}

// checkReady checks if the machine is ready to serve cluster requests (store synced, cluster components started).
func (c *Cluster) checkReady() error {
	select {
	case <-c.ready:
		return nil
	default:
		return status.Error(codes.Unavailable, "machine is not ready to serve cluster requests")
	}
}

// AddMachine adds a machine to the cluster.
func (c *Cluster) AddMachine(ctx context.Context, req *pb.AddMachineRequest) (*pb.AddMachineResponse, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}

	// A machine added through the API is registered as PENDING until the remote machine actually joins.
	return c.addMachine(ctx, req, pb.JoinState_JOIN_STATE_PENDING)
}

// AddMachineWithoutReadyCheck adds a machine to the cluster without checking if the cluster is ready.
// This is used internally during cluster initialisation to add the first machine, which is active
// immediately as it initialises the cluster on itself.
func (c *Cluster) AddMachineWithoutReadyCheck(
	ctx context.Context, req *pb.AddMachineRequest,
) (*pb.AddMachineResponse, error) {
	return c.addMachine(ctx, req, pb.JoinState_JOIN_STATE_ACTIVE)
}

// addMachine registers a machine membership bound to a join identity. A repeated request with the same
// join_id returns the existing membership (idempotent replay), which makes a lost registration response
// safe to retry.
func (c *Cluster) addMachine(
	ctx context.Context, req *pb.AddMachineRequest, initialState pb.JoinState,
) (*pb.AddMachineResponse, error) {
	if req.Name != "" {
		if err := api.ValidateMachineName(req.Name); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	if req.Network == nil {
		return nil, status.Error(codes.InvalidArgument, "network not set")
	}
	if err := req.Network.Validate(); err != nil {
		return nil, err
	}
	if len(req.Network.Endpoints) == 0 {
		return nil, status.Error(codes.InvalidArgument, "endpoints not set")
	}
	if req.PublicIp != nil {
		ip, err := req.PublicIp.ToAddr()
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid public IP: %v", err)
		}
		if !ip.IsValid() {
			return nil, status.Error(codes.InvalidArgument, "invalid public IP")
		}
	}

	joinID := req.JoinId
	if joinID == "" {
		// Legacy clients don't provide an idempotency key. Generate one for the new membership so the
		// remote machine still joins with a bound generation and join identity.
		id, err := secret.NewID()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "generate join ID: %v", err)
		}
		joinID = id
	}

	machines, err := c.store.ListMachines(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list machines: %v", err)
	}

	// Idempotent replay: a membership already registered by this join attempt must be returned as-is so a
	// retry after a lost response continues with the same machine ID, subnet and generation instead of
	// failing as "already exists".
	if req.JoinId != "" {
		if existing := slices.IndexFunc(machines, func(m *pb.MachineInfo) bool {
			return m.JoinId == req.JoinId
		}); existing >= 0 {
			m := machines[existing]
			if !bytes.Equal(m.Network.PublicKey, req.Network.PublicKey) {
				return nil, status.Errorf(codes.InvalidArgument,
					"join ID %q is already bound to a machine with a different public key", req.JoinId)
			}
			if req.Name != "" && m.Name != req.Name {
				return nil, status.Errorf(codes.InvalidArgument,
					"join ID %q is already bound to machine %q, not %q", req.JoinId, m.Name, req.Name)
			}
			return &pb.AddMachineResponse{Machine: m}, nil
		}
	}

	for _, m := range machines {
		if req.Name != "" && m.Name == req.Name {
			return nil, status.Errorf(codes.AlreadyExists, "machine with name %q already exists", req.Name)
		}
		if req.Network.ManagementIp != nil && req.Network.ManagementIp.Equal(m.Network.ManagementIp) {
			manageIP, _ := req.Network.ManagementIp.ToAddr()
			return nil, status.Errorf(
				codes.AlreadyExists, "machine with management IP %q already exists under the name %q",
				manageIP, m.Name,
			)
		}
		if bytes.Equal(m.Network.PublicKey, req.Network.PublicKey) {
			publicKey := secret.Secret(m.Network.PublicKey)
			return nil, status.Errorf(
				codes.AlreadyExists, "machine with public key %q already exists under the name %q",
				publicKey, m.Name,
			)
		}
	}

	mid, err := NewMachineID()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "generate machine ID: %v", err)
	}
	name := req.Name
	if name == "" {
		if name, err = NewRandomMachineName(); err != nil {
			return nil, status.Errorf(codes.Internal, "generate machine name: %v", err)
		}
	}
	manageIP := req.Network.ManagementIp
	if manageIP == nil {
		manageIP = pb.NewIP(network.ManagementIP(req.Network.PublicKey))
	}
	// Allocate a subnet for the machine from the cluster network.
	clusterNetwork, err := c.network(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get cluster network: %v", err)
	}
	allocatedSubnets := make([]netip.Prefix, len(machines))
	for i, m := range machines {
		allocatedSubnets[i], _ = m.Network.Subnet.ToPrefix()
	}
	ipam, err := NewIPAMWithAllocated(clusterNetwork, allocatedSubnets)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create IPAM manager: %v", err)
	}
	subnet, err := ipam.AllocateSubnetLen(DefaultSubnetBits)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "allocate subnet for machine: %v", err)
	}

	m := &pb.MachineInfo{
		Id:   mid,
		Name: name,
		Network: &pb.NetworkConfig{
			Subnet:       pb.NewIPPrefix(subnet),
			ManagementIp: manageIP,
			Endpoints:    req.Network.Endpoints,
			PublicKey:    req.Network.PublicKey,
		},
		PublicIp:   req.PublicIp,
		Generation: nextMemberGeneration(machines),
		JoinId:     joinID,
		JoinState:  initialState,
		ClusterId:  c.clusterID(ctx),
	}
	// TODO: announce the new machine to the cluster members and achieve consensus.
	//  We should perhaps not proceed if this machine is in a minority partition.
	if err = c.store.CreateMachine(ctx, m); err != nil {
		return nil, status.Errorf(codes.Internal, "create machine: %v", err)
	}
	slog.Info("Machine registered in the cluster.",
		"id", m.Id, "name", m.Name, "subnet", subnet, "generation", m.Generation,
		"join_id", m.JoinId, "state", m.JoinState, "public_key", secret.Secret(m.Network.PublicKey))

	// Arbitrate registrations that raced with this one (same name, public key or management IP).
	if err = c.resolveRegistrationConflicts(ctx, m); err != nil {
		return nil, err
	}

	resp := &pb.AddMachineResponse{Machine: m}
	return resp, nil
}

// resolveRegistrationConflicts re-reads the machine list and arbitrates duplicate registrations committed
// concurrently (potentially on different cluster members). An ACTIVE member always wins. Between two PENDING
// registrations, the older membership generation (ties broken by join ID) owns the identity and the losing
// PENDING record is revoked so it does not leak its subnet.
func (c *Cluster) resolveRegistrationConflicts(ctx context.Context, registered *pb.MachineInfo) error {
	conflict := c.findConflictingMember(ctx, registered)
	if conflict == nil && api.IsPendingJoin(registered) {
		// PENDING registrations can race across cluster members. Give a registration committed on another
		// member a moment to replicate before deciding this attempt won the identity. The first (ACTIVE)
		// member initialises a fresh cluster by itself, so there is nobody to race with.
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-time.After(concurrentRegistrationSettle):
		}
		conflict = c.findConflictingMember(ctx, registered)
	}
	if conflict == nil {
		return nil
	}

	loser := pickRegistrationLoser(registered, conflict)
	if loser == registered {
		if err := c.store.DeleteMachine(ctx, registered.Id); err != nil &&
			!errors.Is(err, store.ErrMachineNotFound) {
			return status.Errorf(codes.Internal, "revoke losing machine registration: %v", err)
		}
		slog.Info("Revoked machine registration that lost a concurrent registration conflict.",
			"id", registered.Id, "name", registered.Name, "generation", registered.Generation,
			"winner_id", conflict.Id, "winner_generation", conflict.Generation)
		return membershipConflictStatus(conflict)
	}

	// This registration won. The loser can only be PENDING here (an ACTIVE conflict always wins) so revoke
	// it to release its subnet.
	if api.IsPendingJoin(loser) {
		if err := c.store.DeleteMachine(ctx, loser.Id); err != nil &&
			!errors.Is(err, store.ErrMachineNotFound) {
			slog.Error("Failed to revoke the stale PENDING machine registration, its subnet may leak temporarily.",
				"id", loser.Id, "err", err)
		} else {
			slog.Info("Revoked stale PENDING machine registration after winning the conflict.",
				"id", loser.Id, "name", loser.Name, "generation", loser.Generation)
		}
	}
	return nil
}

// findConflictingMember re-reads the store and returns another machine claiming the same name, management
// IP or public key as the registered one.
func (c *Cluster) findConflictingMember(ctx context.Context, registered *pb.MachineInfo) *pb.MachineInfo {
	machines, err := c.store.ListMachines(ctx)
	if err != nil {
		slog.Error("Failed to list machines while arbitrating concurrent registrations.", "err", err)
		return nil
	}
	if other, _ := findConflictingMember(registered, machines); other != nil {
		return other
	}
	return nil
}

func membershipConflictStatus(m *pb.MachineInfo) error {
	return status.Errorf(codes.AlreadyExists,
		"machine with name %q, generation %d and join ID %q holds the conflicting identity",
		m.Name, m.Generation, m.JoinId)
}

// findConflictingMember returns the first machine other than registered claiming the same name, management
// IP or public key.
func findConflictingMember(
	registered *pb.MachineInfo, others []*pb.MachineInfo,
) (conflict *pb.MachineInfo, field string) {
	for _, other := range others {
		if other.Id == registered.Id {
			continue
		}
		if other.Name == registered.Name {
			return other, "name"
		}
		if registered.Network.ManagementIp != nil &&
			registered.Network.ManagementIp.Equal(other.Network.ManagementIp) {
			return other, "management IP"
		}
		if bytes.Equal(other.Network.PublicKey, registered.Network.PublicKey) {
			return other, "public key"
		}
	}
	return nil, ""
}

// pickRegistrationLoser decides which of two conflicting registrations must be revoked. An ACTIVE membership
// always beats a PENDING one. Between two PENDING memberships the older generation wins; the join ID breaks
// ties deterministically.
func pickRegistrationLoser(a, b *pb.MachineInfo) *pb.MachineInfo {
	if api.IsPendingJoin(a) && !api.IsPendingJoin(b) {
		return a
	}
	if !api.IsPendingJoin(a) && api.IsPendingJoin(b) {
		return b
	}
	if a.Generation != b.Generation {
		if a.Generation < b.Generation {
			return b
		}
		return a
	}
	if a.JoinId < b.JoinId {
		return b
	}
	return a
}

// nextMemberGeneration allocates a membership generation that is unique and increasing across the cluster.
// It is a hybrid logical clock: the wall clock orders registrations made on different cluster members, and
// the maximum observed generation keeps it strictly increasing locally even when the clock moves backwards.
func nextMemberGeneration(existing []*pb.MachineInfo) uint64 {
	var maxGen uint64
	for _, m := range existing {
		if m.Generation > maxGen {
			maxGen = m.Generation
		}
	}

	gen := uint64(time.Now().UnixMilli())<<memberGenerationRandomBits |
		rand.Uint64()&(1<<memberGenerationRandomBits-1)
	if gen <= maxGen {
		gen = maxGen + 1
	}
	return gen
}

func (c *Cluster) network(ctx context.Context) (netip.Prefix, error) {
	var net string
	if err := c.store.Get(ctx, "network", &net); err != nil {
		return netip.Prefix{}, status.Errorf(codes.Internal, "get network from store: %v", err)
	}
	prefix, err := netip.ParsePrefix(net)
	if err != nil {
		return netip.Prefix{}, status.Errorf(codes.Internal, "parse network prefix: %v", err)
	}
	return prefix, nil
}

// clusterID returns the unique ID of this cluster, or an empty string for legacy clusters initialised
// before cluster IDs were introduced.
func (c *Cluster) clusterID(ctx context.Context) string {
	var id string
	if err := c.store.Get(ctx, clusterIDKey, &id); err != nil {
		if !errors.Is(err, store.ErrKeyNotFound) {
			slog.Error("Failed to get cluster ID from the store.", "err", err)
		}
		return ""
	}
	return id
}

// ListMachines lists all machines in the cluster including their membership states.
func (c *Cluster) ListMachines(ctx context.Context, _ *emptypb.Empty) (*pb.ListMachinesResponse, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}

	machines, err := c.store.ListMachines(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	states, err := c.corroAdmin.ClusterMembershipStates(true)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get cluster membership states: %v", err)
	}

	members := make([]*pb.MachineMember, len(machines))
	for i, m := range machines {
		// If the machine is not in the cluster membership states or its state is not ALIVE or SUSPECT, it is DOWN.
		// The exception is the current machine which is always UP as it is serving this request.
		state := pb.MachineMember_DOWN
		addr, _ := m.Network.ManagementIp.ToAddr()
		for _, s := range states {
			if s.Addr.Addr().Compare(addr) == 0 {
				switch s.State {
				case corrosion.MembershipStateAlive:
					state = pb.MachineMember_UP
				case corrosion.MembershipStateSuspect:
					state = pb.MachineMember_SUSPECT
				}
				break
			}
		}
		// If the machine is the current machine, it is UP.
		if m.Id == c.machineID {
			state = pb.MachineMember_UP
		}
		members[i] = &pb.MachineMember{
			Machine: m,
			State:   state,
		}
	}

	return &pb.ListMachinesResponse{Machines: members}, nil
}

// RemoveMachine removes a machine from the cluster.
func (c *Cluster) RemoveMachine(ctx context.Context, req *pb.RemoveMachineRequest) (*emptypb.Empty, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}

	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "machine ID not set")
	}

	// A guarded removal is used by a failed add attempt to revoke a PENDING membership it registered
	// itself. It must not touch a membership that already became ACTIVE or that belongs to a newer
	// generation (removed and re-added by another attempt, or reset).
	if req.JoinId != "" {
		m, err := c.store.GetMachine(ctx, req.Id)
		if err != nil {
			if errors.Is(err, store.ErrMachineNotFound) {
				return nil, status.Errorf(codes.NotFound, "machine not found: %s", req.Id)
			}
			return nil, status.Errorf(codes.Internal, "get machine: %v", err)
		}
		if m.JoinId != req.JoinId || m.Generation != req.Generation {
			return nil, status.Errorf(codes.FailedPrecondition,
				"machine %q membership is not the PENDING registration of add attempt %q (current generation %d, "+
					"join ID %q): the add attempt is superseded and must not revoke it",
				m.Name, req.JoinId, m.Generation, m.JoinId)
		}
		if !api.IsPendingJoin(m) {
			return nil, status.Errorf(codes.FailedPrecondition,
				"machine %q has already joined the cluster and cannot be revoked by add attempt %q",
				m.Name, req.JoinId)
		}
	}

	// Cleanup machine containers from the store that could be left if the machine is unavailable
	// removed with --no-reset, or didn't have time to finish propagating changes before resetting.
	if err := c.store.DeleteContainers(ctx, store.DeleteOptions{MachineIDs: []string{req.Id}}); err != nil {
		slog.Error("Failed to delete container records from the cluster store for the machine being removed.",
			"id", req.Id, "err", err)
	}

	if err := c.store.DeleteMachine(ctx, req.Id); err != nil {
		if errors.Is(err, store.ErrMachineNotFound) {
			return nil, status.Errorf(codes.NotFound, "machine not found: %s", req.Id)
		}
		return nil, status.Errorf(codes.Internal, "delete machine from store: %v", err)
	}
	if req.JoinId != "" {
		slog.Info("Revoked a PENDING machine membership after a failed add attempt.",
			"id", req.Id, "generation", req.Generation, "join_id", req.JoinId)
	} else {
		slog.Info("Machine removed from the cluster.", "id", req.Id)
	}

	return &emptypb.Empty{}, nil
}

// MemberRTTs returns the median and standard deviation of round-trip times from this member to each cluster member.
func (c *Cluster) MemberRTTs() ([]corrosion.MemberRTTStats, error) {
	return c.corroAdmin.ClusterMemberRTTs()
}
