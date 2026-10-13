package cli

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/psviderski/uncloud/api/pb"
	"github.com/psviderski/uncloud/internal/cli/config"
	"github.com/psviderski/uncloud/internal/secret"
	"github.com/psviderski/uncloud/pkg/api"
	"github.com/psviderski/uncloud/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// newMachineJoin creates a fresh add attempt with a new join identity and the fixed remote facts.
func newMachineJoin(
	contextName string, opts AddMachineOptions, machineName string,
	publicKey []byte, publicIP *pb.IP, endpoints []*pb.IPPort,
) (*machineJoin, error) {
	joinID, err := secret.NewID()
	if err != nil {
		return nil, fmt.Errorf("generate join ID: %w", err)
	}
	return &machineJoin{
		JoinID:        joinID,
		ContextName:   contextName,
		Remote:        remoteJoinHostFromMachine(opts.RemoteMachine),
		SkipInstall:   opts.SkipInstall,
		Version:       opts.Version,
		PublicKey:     publicKey,
		PublicIP:      publicIP,
		Endpoints:     endpoints,
		Name:          machineName,
		WireguardPort: opts.WireguardPort,
		WireguardMTU:  opts.WireguardMTU,
	}, nil
}

// pendingMemberWithPublicKey returns the PENDING member registered for the given public key, if any.
func pendingMemberWithPublicKey(members api.MachineMembersList, publicKey []byte) *pb.MachineInfo {
	for _, member := range members {
		if api.IsPendingJoin(member.Machine) &&
			slices.Equal(member.Machine.Network.GetPublicKey(), publicKey) {
			return member.Machine
		}
	}
	return nil
}

// activeMemberWithPublicKey returns the active (or legacy) member registered for the given public key.
func activeMemberWithPublicKey(members api.MachineMembersList, publicKey []byte) *pb.MachineInfo {
	for _, member := range members {
		if !api.IsPendingJoin(member.Machine) &&
			slices.Equal(member.Machine.Network.GetPublicKey(), publicKey) {
			return member.Machine
		}
	}
	return nil
}

// isAmbiguousGRPCError reports whether the error means the server-side outcome of the call is unknown
// (the call may or may not have taken effect), so it is safe to replay an idempotent request.
func isAmbiguousGRPCError(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.ResourceExhausted, codes.Aborted:
		return true
	}
	return false
}

// registerMachineForJoin registers the machine bound to the join identity, recovering from a lost
// registration response and adopting an existing PENDING registration for the same remote machine instead
// of failing as "already exists".
func registerMachineForJoin(
	ctx context.Context, c *client.Client, join *machineJoin,
) (*pb.MachineInfo, error) {
	resp, err := c.AddMachine(ctx, join.addRequest())
	if err == nil {
		return resp.Machine, nil
	}
	registerErr := err

	// The response may have been lost after the server committed the registration. Replaying the same join
	// ID returns the existing membership when the first call landed.
	if isAmbiguousGRPCError(registerErr) {
		if resp, rErr := c.AddMachine(ctx, join.addRequest()); rErr == nil {
			return resp.Machine, nil
		}
	}

	// Determine the actual cluster facts. A PENDING member with the same public key is the same (still
	// uninitialised) remote machine whose registration never took effect, so adopt it and continue.
	members, listErr := c.ListMachines(ctx, nil)
	if listErr != nil {
		return nil, registerErr
	}
	if pending := pendingMemberWithPublicKey(members, join.PublicKey); pending != nil {
		if join.Name != "" && pending.Name != join.Name {
			// The remote machine is being added under two different names: do not silently pick one.
			return nil, registerErr
		}
		return pending, nil
	}
	return nil, registerErr
}

// snapshotJoinMembership captures the minimum store version the new machine must catch up with and the
// current other members snapshot, binding both to the join identity.
func snapshotJoinMembership(
	ctx context.Context, c *client.Client, join *machineJoin,
) error {
	var storeVersion api.StoreVersion
	inspectResp, err := c.MachineClient.InspectMachine(ctx, &emptypb.Empty{})
	if err != nil {
		// TODO(lhf): remove Unimplemented check when v0.17.0 is released.
		if status.Convert(err).Code() != codes.Unimplemented {
			return fmt.Errorf("inspect current cluster machine: %w", err)
		}
	} else {
		storeVersion = inspectResp.Machines[0].StoreVersion
	}

	members, err := c.ListMachines(ctx, nil)
	if err != nil {
		return fmt.Errorf("list cluster machines: %w", err)
	}
	otherMachines := make([]*pb.MachineInfo, 0, len(members))
	for _, member := range members {
		if member.Machine.Id != join.Machine.Id {
			otherMachines = append(otherMachines, member.Machine)
		}
	}

	join.MinStoreVersion = storeVersion
	join.OtherMachines = otherMachines
	return nil
}

// remoteMachineInfo fetches the current facts of the remote machine.
func remoteMachineInfo(ctx context.Context, machineClient *client.Client) (*pb.MachineInfo, error) {
	resp, err := machineClient.MachineClient.InspectMachine(ctx, nil)
	if err != nil {
		return nil, err
	}
	return resp.Machines[0].Machine, nil
}

// abortOutcome describes the result of revoking a PENDING membership after the remote join failed.
type abortOutcome int

const (
	abortRevoked abortOutcome = iota
	abortNotFound
	abortSuperseded
)

// abortPendingRegistration revokes the PENDING membership of a failed add attempt. The removal is guarded
// by the join ID and generation, so an ACTIVE membership or a newer generation (removed and re-added,
// reset) is never touched.
func abortPendingRegistration(
	ctx context.Context, c *client.Client, join *machineJoin,
) abortOutcome {
	if join.Machine == nil || join.JoinID == "" || join.Machine.Generation == 0 {
		return abortNotFound
	}
	_, err := c.RemoveMachine(ctx, &pb.RemoveMachineRequest{
		Id:         join.Machine.Id,
		JoinId:     join.JoinID,
		Generation: join.Machine.Generation,
	})
	switch status.Code(err) {
	case codes.OK:
		return abortRevoked
	case codes.NotFound:
		return abortNotFound
	default:
		// FailedPrecondition: the membership is ACTIVE or belongs to a newer generation. Leave it alone.
		return abortSuperseded
	}
}

// joinClusterForJoin drives the remote join and reconciles failures based on the actual remote facts:
// a join that took effect despite a lost response continues, a registration that can never take effect is
// revoked while it still belongs to this attempt, and transient failures leave the attempt resumable.
func joinClusterForJoin(
	ctx context.Context, c *client.Client, machineClient *client.Client,
	joins *joinStore, join *machineJoin,
) error {
	_, err := machineClient.JoinCluster(ctx, join.joinRequest())
	if err == nil {
		return nil
	}
	joinErr := err

	// Inspect the remote to learn whether the join took effect despite the error (lost response, or an
	// idempotent replay handled by the daemon).
	remote, inspectErr := remoteMachineInfo(ctx, machineClient)
	if inspectErr == nil && remote.Id == join.Machine.Id &&
		(join.Machine.Generation == 0 || remote.Generation == join.Machine.Generation) {
		fmt.Printf("The remote machine '%s' has already joined the cluster as generation %d, continuing.\n",
			join.Machine.Name, join.Machine.Generation)
		return nil
	}

	// revokePending abandons the membership registered for this attempt. The guarded removal never touches
	// an ACTIVE membership or a newer generation. The local progress file is dropped only when the removal
	// confirms the bound identity is gone, so the next retry starts a fresh generation.
	revokePending := func() error {
		outcome := abortPendingRegistration(ctx, c, join)
		if outcome != abortSuperseded {
			if rErr := joins.remove(join.JoinID); rErr != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to remove add progress file: %v\n", rErr)
			}
		}
		if outcome == abortSuperseded {
			return fmt.Errorf("join cluster: a newer membership replaced this add attempt, the remote machine "+
				"must be reset before retrying: %w", joinErr)
		}
		return nil
	}

	switch status.Code(joinErr) {
	case codes.FailedPrecondition:
		// The remote is reachable and already configured as another member. Our registration can never
		// take effect, so revoke it while it is still bound to this attempt.
		if rErr := revokePending(); rErr != nil {
			return rErr
		}
		return fmt.Errorf("join cluster: the remote machine is already configured as a different cluster "+
			"member: %w", joinErr)
	case codes.InvalidArgument:
		if strings.Contains(joinErr.Error(), "public key") {
			// The remote host now holds a different key pair (it was reset), so this registration is
			// permanently ineffective.
			if rErr := revokePending(); rErr != nil {
				return rErr
			}
			return fmt.Errorf("join cluster: the remote machine identity changed (it was likely reset); "+
				"revoked its pending registration, please retry: %w", joinErr)
		}
		// A malformed join request is a client bug: do not revoke, keep the attempt resumable.
		return fmt.Errorf("join cluster: %w", joinErr)
	default:
		// Transient/ambiguous failure. The attempt stays queryable and resumable with the same join
		// identity; no membership is revoked because the join may still be landing on the remote.
		return fmt.Errorf("join cluster: %w\nThe add can be safely retried and will resume with the same "+
			"machine ID, subnet and membership generation", joinErr)
	}
}

// recordMachineConnection saves the remote machine's SSH connection in the cluster context, skipping the
// write when it is already recorded (a previous add attempt may have saved it after a successful join).
func (cli *CLI) recordMachineConnection(contextName string, remote *RemoteMachine, machineID string) error {
	cfg, ok := cli.Config.Contexts[contextName]
	if !ok {
		return fmt.Errorf("cluster context %q not found in the Uncloud config", contextName)
	}
	for _, conn := range cfg.Connections {
		if conn.MachineID == machineID {
			return nil
		}
	}

	connCfg := config.MachineConnection{
		SSHKeyFile: remote.KeyPath,
		MachineID:  machineID,
	}
	if remote.UseSSHGo {
		connCfg.SSHGo = config.NewSSHDestination(remote.User, remote.Host, remote.Port)
	} else {
		connCfg.SSH = config.NewSSHDestination(remote.User, remote.Host, remote.Port)
	}
	cfg.Connections = append(cfg.Connections, connCfg)
	if err := cli.Config.Save(); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	return nil
}
