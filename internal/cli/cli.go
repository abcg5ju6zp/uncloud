package cli

import (
	"context"
	"fmt"
	"net/netip"
	"os"

	"github.com/docker/cli/cli/streams"
	"github.com/psviderski/uncloud/api/pb"
	"github.com/psviderski/uncloud/internal/cli/config"
	"github.com/psviderski/uncloud/internal/machine"
	"github.com/psviderski/uncloud/internal/machine/cluster"
	"github.com/psviderski/uncloud/internal/machine/network"
	"github.com/psviderski/uncloud/internal/secret"
	"github.com/psviderski/uncloud/internal/sshexec"
	"github.com/psviderski/uncloud/pkg/api"
	"github.com/psviderski/uncloud/pkg/client"
	"github.com/psviderski/uncloud/pkg/client/connector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	// DefaultSSHKeyPath is the fallback location for the SSH private key when provisioning remote machines.
	// Used when no key is explicitly provided and SSH agent authentication fails.
	DefaultSSHKeyPath  = "~/.ssh/id_ed25519"
	DefaultContextName = "default"
)

type CLI struct {
	Config          *config.Config
	conn            *config.MachineConnection
	contextOverride string
}

// New creates a new CLI instance with the given config path or remote machine connection.
// If the connection is provided, the config is ignored for all operations which is useful for interacting with
// a cluster without creating a config.
// If a non-empty context name is given, it will override the current default.
func New(configPath string, conn *config.MachineConnection, contextName string) (*CLI, error) {
	if conn != nil {
		return &CLI{conn: conn}, nil
	}

	cfg, err := config.NewFromFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read Uncloud config: %w", err)
	}

	return &CLI{
		Config:          cfg,
		contextOverride: contextName,
	}, nil
}

func (cli *CLI) CreateContext(name string) error {
	if _, ok := cli.Config.Contexts[name]; ok {
		return fmt.Errorf("context '%s' already exists", name)
	}
	cli.Config.Contexts[name] = &config.Context{
		Name: name,
	}
	return cli.Config.Save()
}

func (cli *CLI) SetCurrentContext(name string) error {
	if _, ok := cli.Config.Contexts[name]; !ok {
		return api.ErrNotFound
	}
	cli.Config.CurrentContext = name
	return cli.Config.Save()
}

func (cli *CLI) ContextOverrideOrCurrent() string {
	if cli.contextOverride != "" {
		return cli.contextOverride
	}
	if cli.Config != nil {
		return cli.Config.CurrentContext
	}
	return ""
}

// DirectConnection returns the connection string if --connect or UNCLOUD_CONNECT was specified.
func (cli *CLI) DirectConnection() string {
	if cli.conn != nil {
		return cli.conn.String()
	}
	return ""
}

// ConnectCluster connects to a cluster using the context override or the current context if not specified.
// If the CLI was initialised with a machine connection, the config is ignored and the connection is used instead.
func (cli *CLI) ConnectCluster(ctx context.Context) (*client.Client, error) {
	return cli.ConnectClusterWithOptions(ctx, ConnectOptions{
		// Default to showing progress for CLI usage.
		ShowProgress: true,
	})
}

// ConnectClusterWithOptions connects to a cluster with the given options.
// If the CLI was initialised with a machine connection, the config is ignored and the connection is used instead.
// If the CLI has an override context, it is used instead of the current default.
// Options are useful when using the CLI as a library where you may want to disable visual feedback.
func (cli *CLI) ConnectClusterWithOptions(ctx context.Context, opts ConnectOptions) (*client.Client, error) {
	if cli.conn != nil {
		return ConnectCluster(ctx, *cli.conn, opts)
	}

	if len(cli.Config.Contexts) == 0 {
		return nil, fmt.Errorf(
			"no cluster contexts found in the Uncloud config (%s). "+
				"Please initialise a cluster with 'uncloud machine init' first",
			cli.Config.Path(),
		)
	}

	contextName := cli.contextOverride
	if contextName == "" {
		// If the cluster is not specified, use the current cluster if set.
		if cli.Config.CurrentContext == "" {
			return nil, fmt.Errorf(
				"the current cluster context is not set in the Uncloud config (%s). "+
					"Please specify the context with the '--context' flag or set 'current_context' in the config",
				cli.Config.Path(),
			)
		}
		if _, ok := cli.Config.Contexts[cli.Config.CurrentContext]; !ok {
			return nil, fmt.Errorf(
				"current cluster context '%s' not found in the Uncloud config (%s). "+
					"Please specify the context with the '--context' flag or update 'current_context' in the config",
				cli.Config.CurrentContext,
				cli.Config.Path(),
			)
		}
		contextName = cli.Config.CurrentContext
	}

	cfg, ok := cli.Config.Contexts[contextName]
	if !ok {
		return nil, fmt.Errorf("cluster context '%s' not found in the Uncloud config (%s)",
			contextName, cli.Config.Path())
	}
	if len(cfg.Connections) == 0 {
		return nil, fmt.Errorf(
			"no connection configurations found for cluster context '%s' in the Uncloud config (%s)",
			contextName, cli.Config.Path(),
		)
	}

	// Try each connection in order until one succeeds.
	var lastErr error
	for _, conn := range cfg.Connections {
		c, err := ConnectCluster(ctx, conn, opts)
		if err == nil {
			return c, nil
		}

		lastErr = err
	}

	return nil, fmt.Errorf("failed to connect to cluster context '%s': "+
		"all connections (%d) in the Uncloud config (%s) failed; last error: %w",
		contextName, len(cfg.Connections), cli.Config.Path(), lastErr)
}

type InitClusterOptions struct {
	Context            string
	MachineName        string
	Network            netip.Prefix
	PublicIP           *netip.Addr
	RemoteMachine      *RemoteMachine
	SkipInstall        bool
	Version            string
	AutoConfirm        bool
	WireguardEndpoints []*pb.IPPort
	WireguardMTU       int
	WireguardPort      int
}

// InitCluster initialises a new cluster on a remote machine and returns a client to interact with the cluster.
// The client should be closed after use by the caller.
func (cli *CLI) InitCluster(ctx context.Context, opts InitClusterOptions) (*client.Client, error) {
	if opts.RemoteMachine != nil {
		return cli.initRemoteMachine(ctx, opts)
	}
	// TODO: implement local machine initialisation
	return nil, fmt.Errorf("local machine initialisation is not implemented yet. Please specify a remote machine")
}

func (cli *CLI) initRemoteMachine(ctx context.Context, opts InitClusterOptions) (*client.Client, error) {
	contextName, err := cli.newContextName(opts.Context)
	if err != nil {
		return nil, err
	}

	machineClient, err := provisionOrConnectRemoteMachine(ctx, opts.RemoteMachine, opts.SkipInstall, opts.Version)
	if err != nil {
		return nil, err
	}
	// Ensure machineClient is closed on error.
	defer func() {
		if err != nil {
			machineClient.Close()
		}
	}()

	// Check if the machine is already initialised as a cluster member and prompt the user to reset it first.
	minfo, err := machineClient.Inspect(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, fmt.Errorf("inspect machine: %w", err)
	}
	if minfo.Id != "" {
		if !opts.AutoConfirm {
			if err = promptResetMachine(); err != nil {
				return nil, err
			}
		}
		if err = resetAndWaitMachine(ctx, machineClient.MachineClient); err != nil {
			return nil, err
		}
	}

	// Check machine meets all necessary system requirements before proceeding.
	checkResp, err := machineClient.CheckPrerequisites(ctx, &emptypb.Empty{})
	// TODO(lhf): remove Unimplemented check when v0.9.0 is released.
	if err != nil {
		if status.Convert(err).Code() != codes.Unimplemented {
			return nil, fmt.Errorf("check machine prerequisites: %w", err)
		}
	} else if !checkResp.Satisfied {
		return nil, fmt.Errorf("machine prerequisites not satisfied: %s", checkResp.Error)
	}

	req := &pb.InitClusterRequest{
		MachineName:        opts.MachineName,
		Network:            pb.NewIPPrefix(opts.Network),
		WireguardEndpoints: opts.WireguardEndpoints,
		WireguardMtu:       int32(opts.WireguardMTU),
		WireguardPort:      int32(opts.WireguardPort),
	}
	if opts.PublicIP != nil {
		if opts.PublicIP.IsValid() {
			req.PublicIpConfig = &pb.InitClusterRequest_PublicIp{PublicIp: pb.NewIP(*opts.PublicIP)}
		} else {
			// Invalid or in other words zero IP means to automatically detect the public IP.
			req.PublicIpConfig = &pb.InitClusterRequest_PublicIpAuto{PublicIpAuto: true}
		}
	}

	resp, err := machineClient.InitCluster(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("init cluster: %w", err)
	}
	fmt.Printf("Cluster initialised with machine '%s' and saved as context '%s' in your local config (%s)\n",
		resp.Machine.Name, contextName, cli.Config.Path())
	if err = cli.CreateContext(contextName); err != nil {
		return nil, fmt.Errorf("save cluster context to config: %w", err)
	}
	if err = cli.SetCurrentContext(contextName); err != nil {
		return nil, fmt.Errorf("set current cluster context: %w", err)
	}
	fmt.Printf("Current cluster context is now '%s'.\n", contextName)

	// Save the machine's SSH connection details in the context config.
	connCfg := config.MachineConnection{
		SSHKeyFile: opts.RemoteMachine.KeyPath,
		MachineID:  resp.Machine.Id,
	}
	if opts.RemoteMachine.UseSSHGo {
		connCfg.SSHGo = config.NewSSHDestination(
			opts.RemoteMachine.User,
			opts.RemoteMachine.Host,
			opts.RemoteMachine.Port,
		)
	} else {
		connCfg.SSH = config.NewSSHDestination(
			opts.RemoteMachine.User,
			opts.RemoteMachine.Host,
			opts.RemoteMachine.Port,
		)
	}
	cli.Config.Contexts[contextName].Connections = append(cli.Config.Contexts[contextName].Connections, connCfg)
	if err = cli.Config.Save(); err != nil {
		return nil, fmt.Errorf("save config: %w", err)
	}
	return machineClient, nil
}

// newContextName returns a unique name for a new cluster context. If the provided name is not DefaultContextName,
// and it's already taken, an error is returned. If the name is not provided or is DefaultContextName, the first
// available name "default[-N]" is returned.
func (cli *CLI) newContextName(name string) (string, error) {
	if name == "" {
		name = DefaultContextName
	}

	if _, exists := cli.Config.Contexts[name]; !exists {
		return name, nil
	}

	// If non-default context already exists, error out.
	if name != DefaultContextName {
		return "", fmt.Errorf("cluster context '%s' already exists", name)
	}

	// The default context already exists, generate a numbered suffix to make it unique.
	for i := 1; ; i++ {
		name = fmt.Sprintf("%s-%d", DefaultContextName, i)
		if _, exists := cli.Config.Contexts[name]; !exists {
			return name, nil
		}
	}
}

type AddMachineOptions struct {
	MachineName        string
	PublicIP           *netip.Addr
	RemoteMachine      *RemoteMachine
	SkipInstall        bool
	Version            string
	AutoConfirm        bool
	WireguardEndpoints []*pb.IPPort
	WireguardMTU       int
	WireguardPort      int
}

// AddMachine provisions a remote machine and adds it to the cluster. It returns a cluster client and a machine client.
// The cluster client is connected to the existing machine in the cluster. It was used to add the new machine to the
// cluster. The machine client is connected to the new machine and can be used to interact with it.
// Both client should be closed after use by the caller.
//
// The add is a resumable operation bound to a join identity. The machine record, IPAM subnet, other members
// snapshot, WireGuard parameters and minimum store version all reference the same identity, so a lost
// registration response, a failed remote join, an unreplicated cluster store or a failed local context
// write are continued based on facts (replay, revoke or backfill) instead of failing as "already exists".
func (cli *CLI) AddMachine(ctx context.Context, opts AddMachineOptions) (_ *client.Client, _ *client.Client, err error) {
	contextName := cli.ContextOverrideOrCurrent()
	c, err := cli.ConnectCluster(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to cluster (context '%s'): %w", contextName, err)
	}
	defer func() {
		if err != nil {
			c.Close()
		}
	}()

	machineClient, err := provisionOrConnectRemoteMachine(ctx, opts.RemoteMachine, opts.SkipInstall, opts.Version)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			machineClient.Close()
		}
	}()
	// IMPORTANT: 'err' is a named return value so the deferred cleanups above observe it on every error return.
	// Do not shadow it with ':=' in a nested scope, or the deferred client cleanup would be skipped on error.

	minfo, err := remoteMachineInfo(ctx, machineClient)
	if err != nil {
		return nil, nil, fmt.Errorf("inspect machine: %w", err)
	}
	if minfo.Id != "" {
		// The remote machine is already initialised. If it is already a member of THIS cluster, the previous
		// add only lost its local tail (e.g. the context write failed): backfill the connection and continue
		// instead of resetting a successfully joined machine.
		machines, listErr := c.ListMachines(ctx, nil)
		if listErr != nil {
			return nil, nil, fmt.Errorf("list cluster machines: %w", listErr)
		}
		if member := machines.FindByNameOrID(minfo.Id); member != nil {
			if rErr := cli.reconcileJoinedMachine(contextName, opts.RemoteMachine, member.Machine); rErr != nil {
				return nil, nil, rErr
			}
			return c, machineClient, nil
		}

		// The machine belongs to another cluster (or is a stale member removed from this one): reset it
		// before continuing, with the same confirmation behaviour as before.
		if !opts.AutoConfirm {
			if err = promptResetMachine(); err != nil {
				return nil, nil, err
			}
		}
		if err = resetAndWaitMachine(ctx, machineClient.MachineClient); err != nil {
			return nil, nil, err
		}
		if minfo, err = remoteMachineInfo(ctx, machineClient); err != nil {
			return nil, nil, fmt.Errorf("inspect machine after reset: %w", err)
		}
	}

	// Check machine meets all necessary system requirements before proceeding.
	checkResp, err := machineClient.CheckPrerequisites(ctx, &emptypb.Empty{})
	// TODO(lhf): remove Unimplemented check when v0.9.0 is released.
	if err != nil {
		if status.Convert(err).Code() != codes.Unimplemented {
			return nil, nil, fmt.Errorf("check machine prerequisites: %w", err)
		}
	} else if !checkResp.Satisfied {
		return nil, nil, fmt.Errorf("machine prerequisites not satisfied: %s", checkResp.Error)
	}

	tokenResp, err := machineClient.Token(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, nil, fmt.Errorf("get remote machine token: %w", err)
	}
	token, err := machine.ParseToken(tokenResp.Token)
	if err != nil {
		return nil, nil, fmt.Errorf("parse remote machine token: %w", err)
	}

	// Register the machine in the cluster using its public key and WireGuard endpoints from the token,
	// or use the explicitly provided WireGuard endpoints if any.
	var endpoints []*pb.IPPort
	if len(opts.WireguardEndpoints) > 0 {
		endpoints = opts.WireguardEndpoints
	} else {
		endpoints = make([]*pb.IPPort, len(token.Endpoints))
		for i, addrPort := range token.Endpoints {
			// If a custom WireGuard port is specified, override the port from the token endpoints
			// since the token was generated before the machine knows its configured port.
			if opts.WireguardPort != 0 && opts.WireguardPort != network.DefaultWireGuardPort {
				addrPort = netip.AddrPortFrom(addrPort.Addr(), uint16(opts.WireguardPort))
			}
			endpoints[i] = pb.NewIPPort(addrPort)
		}
	}
	// Default the machine name to the machine's hostname when not explicitly provided, ensuring it is
	// unique within the cluster.
	machineName := opts.MachineName
	members, err := c.ListMachines(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("list cluster machines: %w", err)
	}
	if machineName == "" {
		existing := make([]string, len(members))
		for i, m := range members {
			existing[i] = m.Machine.Name
		}
		if machineName, err = cluster.DefaultMachineName(minfo.Hostname, existing); err != nil {
			return nil, nil, fmt.Errorf("generate machine name: %w", err)
		}
	}
	var registeredPublicIP *pb.IP
	if opts.PublicIP != nil {
		if opts.PublicIP.IsValid() {
			registeredPublicIP = pb.NewIP(*opts.PublicIP)
		} else if token.PublicIP.IsValid() {
			// Invalid or in other words zero IP means to use an automatically detected public IP from the token.
			registeredPublicIP = pb.NewIP(token.PublicIP)
		}
	}

	joins := newJoinStore(cli.Config.Path())

	// Fix the join identity for this attempt and resume an earlier one when the facts allow.
	join, err := cli.prepareJoin(
		joins, contextName, members, opts, machineName, token.PublicKey, registeredPublicIP, endpoints,
	)
	if err != nil {
		return nil, nil, err
	}

	// Register the membership (idempotent) or adopt the PENDING registration already held by this remote.
	registered, err := registerMachineForJoin(ctx, c, join)
	if err != nil {
		return nil, nil, fmt.Errorf("add machine to cluster (context '%s'): %w", contextName, err)
	}
	join.Machine = registered
	join.ClusterID = registered.ClusterId
	join.Name = registered.Name
	// The endpoints and public IP are fixed by the first registration; keep the attempt consistent with it.
	join.Endpoints = registered.Network.Endpoints
	join.PublicIP = registered.PublicIp
	if err = joins.save(join, joinPhaseRegistered); err != nil {
		return nil, nil, fmt.Errorf("save add progress: %w", err)
	}

	// Bind the member snapshot and minimum store version of THIS membership to the join identity.
	if err = snapshotJoinMembership(ctx, c, join); err != nil {
		return nil, nil, err
	}
	if err = joins.save(join, joinPhaseRegistered); err != nil {
		return nil, nil, fmt.Errorf("save add progress: %w", err)
	}

	// Configure the remote machine to join the cluster. Failures are reconciled from the remote facts.
	if err = joinClusterForJoin(ctx, c, machineClient, joins, join); err != nil {
		return nil, nil, err
	}
	if err = joins.save(join, joinPhaseJoined); err != nil {
		return nil, nil, fmt.Errorf("save add progress: %w", err)
	}

	fmt.Printf("Machine '%s' added to the cluster (context '%s').\n", join.Machine.Name, contextName)

	// Save the machine's SSH connection details in the context config. If this fails, the join stays
	// recorded and a retry backfills the connection without touching the joined machine.
	if err = cli.recordMachineConnection(contextName, opts.RemoteMachine, join.Machine.Id); err != nil {
		return nil, nil, err
	}
	// The add is fully complete: discard the resumable progress.
	if err = joins.remove(join.JoinID); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to remove add progress file: %v\n", err)
	}

	return c, machineClient, nil
}

// prepareJoin creates a fresh join identity or resumes an in-progress add. The cluster store is the source
// of truth for the registration; the local progress file only speeds up resume and keeps bound snapshots.
func (cli *CLI) prepareJoin(
	joins *joinStore, contextName string, members api.MachineMembersList, opts AddMachineOptions,
	machineName string, publicKey []byte, publicIP *pb.IP, endpoints []*pb.IPPort,
) (*machineJoin, error) {
	// A PENDING registration for this exact public key already exists in the cluster: adopt it so the
	// retry continues the original membership instead of colliding with it.
	if pending := pendingMemberWithPublicKey(members, publicKey); pending != nil {
		if opts.MachineName != "" && pending.Name != opts.MachineName {
			return nil, fmt.Errorf("machine with this public key is already being added as %q, not %q",
				pending.Name, opts.MachineName)
		}
		join, err := newMachineJoin(contextName, opts, pending.Name, publicKey, pending.PublicIp,
			pending.Network.Endpoints)
		if err != nil {
			return nil, err
		}
		join.JoinID = pending.JoinId
		join.ClusterID = pending.ClusterId
		join.Machine = pending
		if err = joins.save(join, joinPhaseRegistered); err != nil {
			return nil, fmt.Errorf("save add progress: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Resuming a previous add attempt for machine '%s' (generation %d).\n",
			pending.Name, pending.Generation)
		return join, nil
	}

	// An ACTIVE member with this public key while the remote is uninitialised is an inconsistent state
	// (e.g. the remote state was wiped manually). It must not be reset or duplicated automatically.
	if active := activeMemberWithPublicKey(members, publicKey); active != nil {
		return nil, fmt.Errorf("machine with public key %q is already an active member %q of the cluster, "+
			"but the remote host is not initialised. Remove it from the cluster with 'uc machine rm %s' or "+
			"reinitialise the remote host before adding it again",
			secret.Secret(publicKey), active.Name, active.Name)
	}

	// A local progress file whose registration is gone from the cluster was superseded by a removal:
	// abandon that join identity and start a new generation.
	if stale := joins.findResumable(contextName, publicKey); stale != nil {
		if err := joins.remove(stale.JoinID); err != nil {
			return nil, fmt.Errorf("remove stale add progress: %w", err)
		}
	}

	join, err := newMachineJoin(contextName, opts, machineName, publicKey, publicIP, endpoints)
	if err != nil {
		return nil, err
	}
	if err = joins.save(join, joinPhasePrepared); err != nil {
		return nil, fmt.Errorf("save add progress: %w", err)
	}
	return join, nil
}

// reconcileJoinedMachine completes an add whose remote machine already joined this cluster: it backfills
// the SSH connection in the local context config (if missing) and discards any stale add progress.
func (cli *CLI) reconcileJoinedMachine(
	contextName string, remote *RemoteMachine, m *pb.MachineInfo,
) error {
	if err := cli.recordMachineConnection(contextName, remote, m.Id); err != nil {
		return err
	}
	joins := newJoinStore(cli.Config.Path())
	if err := joins.removeForMachine(contextName, m.Id, m.Network.PublicKey); err != nil {
		// Cleanup of stale progress is best-effort and must not fail the recovered add.
		fmt.Fprintf(os.Stderr, "Warning: failed to clean up add progress file: %v\n", err)
	}
	fmt.Printf("Machine '%s' is already a member of this cluster; recovered its connection in context '%s'.\n",
		m.Name, contextName)
	return nil
}

// provisionOrConnectRemoteMachine installs the Uncloud daemon and dependencies on the remote machine over SSH and
// returns a machine API client to interact with the machine. The client should be closed after use by the caller.
// The version parameter specifies the version of the Uncloud daemon to install. If empty, the latest version is used.
// If skipInstall is true, the installation step is skipped, and it is assumed that the Uncloud daemon and dependencies
// are already installed and running.
// The remoteMachine.SSHKeyPath could be updated to the default SSH key path if it is not set and the SSH agent
// authentication fails.
func provisionOrConnectRemoteMachine(
	ctx context.Context, remoteMachine *RemoteMachine, skipInstall bool, version string,
) (*client.Client, error) {
	// Use Go's built-in SSH library.
	if remoteMachine.UseSSHGo {
		sshClient, err := sshexec.Connect(
			remoteMachine.User, remoteMachine.Host, remoteMachine.Port, remoteMachine.KeyPath,
		)
		// If the SSH connection using SSH agent fails and no key path is provided, try to use the default SSH key.
		if err != nil && remoteMachine.KeyPath == "" {
			remoteMachine.KeyPath = DefaultSSHKeyPath
			sshClient, err = sshexec.Connect(
				remoteMachine.User, remoteMachine.Host, remoteMachine.Port, remoteMachine.KeyPath,
			)
		}
		if err != nil {
			return nil, fmt.Errorf(
				"SSH login to remote machine %s: %w",
				config.NewSSHDestination(remoteMachine.User, remoteMachine.Host, remoteMachine.Port), err,
			)
		}

		if !skipInstall {
			// Provision the remote machine by installing the Uncloud daemon and dependencies over SSH.
			exec := sshexec.NewRemote(sshClient)
			if err = provisionMachine(ctx, exec, version); err != nil {
				return nil, fmt.Errorf("provision machine: %w", err)
			}
		}

		var machineClient *client.Client
		if remoteMachine.User == "root" || skipInstall {
			// Create a machine API client over the established SSH connection to the remote machine.
			machineClient, err = client.New(ctx, connector.NewSSHConnectorFromClient(sshClient))
		} else {
			// Since the user is not root, we need to establish a new SSH connection to make the user's addition
			// to the uncloud group effective, thus allowing access to the Uncloud daemon Unix socket.
			sshConfig := &connector.SSHConnectorConfig{
				User:    remoteMachine.User,
				Host:    remoteMachine.Host,
				Port:    remoteMachine.Port,
				KeyPath: remoteMachine.KeyPath,
			}
			machineClient, err = client.New(ctx, connector.NewSSHConnector(sshConfig))
		}
		if err != nil {
			return nil, fmt.Errorf("connect to remote machine: %w", err)
		}
		return machineClient, nil
	}

	// Use the system 'ssh' command (default).
	sshConfig := &connector.SSHConnectorConfig{
		User:    remoteMachine.User,
		Host:    remoteMachine.Host,
		Port:    remoteMachine.Port,
		KeyPath: remoteMachine.KeyPath,
	}
	conn := connector.NewSSHCLIConnector(sshConfig)

	if !skipInstall {
		exec := sshexec.NewSSHCLIRemote(
			remoteMachine.User,
			remoteMachine.Host,
			remoteMachine.Port,
			remoteMachine.KeyPath,
		)
		if err := provisionMachine(ctx, exec, version); err != nil {
			return nil, fmt.Errorf("provision machine: %w", err)
		}

		if remoteMachine.User != rootUser {
			// provisionMachine has just added the user to the uncloud group. Any SSH ControlMaster left over from
			// a previous uc invocation (e.g. a failed uc command against the uninitialised machine) still holds
			// the old user groups and would deny access to /run/uncloud/api/uncloud.sock. Close the current session
			// if it exists so the next session picks up the new groups.
			conn.CloseControlMaster(ctx)
		}
	}

	machineClient, err := client.New(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("connect to remote machine: %w", err)
	}
	return machineClient, nil
}

// SetClusterContextIfUnset sets the cluster context override only if no --context flag was used
// and no --connect direct connection is active.
func (cli *CLI) SetClusterContextIfUnset(name string) {
	if name == "" || cli.contextOverride != "" || cli.conn != nil {
		return
	}
	cli.contextOverride = name
}

// ProgressOut returns an output stream for progress writer.
func (cli *CLI) ProgressOut() *streams.Out {
	return streams.NewOut(os.Stdout)
}
