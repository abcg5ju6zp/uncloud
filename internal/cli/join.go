package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/psviderski/uncloud/api/pb"
	"github.com/psviderski/uncloud/pkg/api"
)

// joinProgressDir is the directory (relative to the Uncloud config file) where resumable add attempts are
// recorded, one file per join identity.
const joinProgressDir = "joins"

type joinPhase string

const (
	// joinPhasePrepared means the remote host identity, public key, endpoints and WireGuard parameters are
	// fixed and the prerequisites have passed, but the machine is not registered in the cluster yet.
	joinPhasePrepared joinPhase = "prepared"
	// joinPhaseRegistered means the machine membership (ID, subnet, generation, cluster ID) is registered in
	// the cluster store and bound to this join identity.
	joinPhaseRegistered joinPhase = "registered"
	// joinPhaseJoined means the remote machine successfully joined the cluster (verified by its own state).
	joinPhaseJoined joinPhase = "joined"
	// joinPhaseRecorded means the local context connection has been saved.
	joinPhaseRecorded joinPhase = "recorded"
)

// remoteJoinHost identifies the fixed remote host of an add attempt.
type remoteJoinHost struct {
	User     string `json:"user,omitempty"`
	Host     string `json:"host"`
	Port     int    `json:"port,omitempty"`
	KeyPath  string `json:"key_path,omitempty"`
	UseSSHGo bool   `json:"use_ssh_go,omitempty"`
}

func remoteJoinHostFromMachine(rm *RemoteMachine) remoteJoinHost {
	return remoteJoinHost{
		User:     rm.User,
		Host:     rm.Host,
		Port:     rm.Port,
		KeyPath:  rm.KeyPath,
		UseSSHGo: rm.UseSSHGo,
	}
}

// machineJoin is the persisted, queryable progress of one "uc machine add" attempt. All resources of the
// attempt (machine record, IPAM subnet, other members snapshot, WireGuard parameters, minimum store
// version) are bound to JoinID so the add can safely resume after a lost response, a failed remote join,
// an unreplicated store or a failed local context write.
type machineJoin struct {
	// JoinID is the idempotency key generated once for this attempt.
	JoinID string `json:"join_id"`
	// ContextName is the cluster context the machine is being added to.
	ContextName string `json:"context"`
	// ClusterID is captured from the membership at registration.
	ClusterID string `json:"cluster_id,omitempty"`

	// Remote is the fixed remote host identity of the attempt.
	Remote remoteJoinHost `json:"remote"`
	// SkipInstall and Version allow reproducing provisioning on resume.
	SkipInstall bool   `json:"skip_install,omitempty"`
	Version     string `json:"version,omitempty"`

	// PublicKey is the remote machine's WireGuard public key captured from its token.
	PublicKey []byte `json:"public_key"`
	// PublicIP is the resolved ingress public IP registered with the machine (nil disables ingress).
	PublicIP *pb.IP `json:"public_ip,omitempty"`
	// Endpoints are the fixed WireGuard endpoints other members use to reach the machine.
	Endpoints []*pb.IPPort `json:"endpoints"`
	// Name is the machine name bound to the attempt.
	Name string `json:"name,omitempty"`
	// WireguardPort and WireguardMTU are the fixed WireGuard parameters for the remote machine.
	WireguardPort int `json:"wireguard_port,omitempty"`
	WireguardMTU  int `json:"wireguard_mtu,omitempty"`

	// Machine is the registered cluster membership (ID, subnet, generation, join ID) available from the
	// registered phase onwards.
	Machine *pb.MachineInfo `json:"machine,omitempty"`
	// MinStoreVersion and OtherMachines are the join-time snapshots bound to this attempt.
	MinStoreVersion api.StoreVersion   `json:"min_store_version,omitempty"`
	OtherMachines   []*pb.MachineInfo `json:"other_machines,omitempty"`

	Phase     joinPhase `json:"phase"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// addRequest builds the cluster registration request from the fixed attempt data.
func (j *machineJoin) addRequest() *pb.AddMachineRequest {
	req := &pb.AddMachineRequest{
		Name: j.Name,
		Network: &pb.NetworkConfig{
			Endpoints: j.Endpoints,
			PublicKey: j.PublicKey,
		},
		PublicIp: j.PublicIP,
		JoinId:   j.JoinID,
	}
	return req
}

// joinRequest builds the remote join request from the registered membership and bound snapshots.
func (j *machineJoin) joinRequest() *pb.JoinClusterRequest {
	return &pb.JoinClusterRequest{
		Machine:         j.Machine,
		OtherMachines:   j.OtherMachines,
		MinStoreVersion: j.MinStoreVersion,
		WireguardMtu:    int32(j.WireguardMTU),
		WireguardPort:   int32(j.WireguardPort),
		ClusterId:       j.ClusterID,
	}
}

// joinStore persists add attempts as JSON files so they remain queryable and resumable across CLI
// invocations, including after a service restart or a lost local response.
type joinStore struct {
	dir string
}

func newJoinStore(configPath string) *joinStore {
	return &joinStore{dir: filepath.Join(filepath.Dir(configPath), joinProgressDir)}
}

func (s *joinStore) path(joinID string) string {
	return filepath.Join(s.dir, joinID+".json")
}

// save atomically persists the attempt, updating its timestamp and phase.
func (s *joinStore) save(j *machineJoin, phase joinPhase) error {
	if j.JoinID == "" {
		return fmt.Errorf("join ID is not set")
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return fmt.Errorf("create join progress directory %q: %w", s.dir, err)
	}
	now := time.Now().UTC()
	if j.CreatedAt.IsZero() {
		j.CreatedAt = now
	}
	j.UpdatedAt = now
	j.Phase = phase

	data, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal join progress: %w", err)
	}

	tmpFile, err := os.CreateTemp(s.dir, ".join.*.json")
	if err != nil {
		return fmt.Errorf("create join progress temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer func() {
		if err != nil {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err = tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return fmt.Errorf("write join progress temp file: %w", err)
	}
	if err = tmpFile.Close(); err != nil {
		return fmt.Errorf("close join progress temp file: %w", err)
	}
	if err = os.Chmod(tmpPath, 0o600); err != nil {
		return fmt.Errorf("chmod join progress temp file: %w", err)
	}
	if err = os.Rename(tmpPath, s.path(j.JoinID)); err != nil {
		return fmt.Errorf("rename join progress file: %w", err)
	}
	return nil
}

// remove deletes the attempt file. A missing file is not an error.
func (s *joinStore) remove(joinID string) error {
	if err := os.Remove(s.path(joinID)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove join progress file: %w", err)
	}
	return nil
}

// list returns all persisted attempts, newest first. Unreadable files are skipped.
func (s *joinStore) list() ([]*machineJoin, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read join progress directory %q: %w", s.dir, err)
	}

	var joins []*machineJoin
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		data, rErr := os.ReadFile(filepath.Join(s.dir, entry.Name()))
		if rErr != nil {
			continue
		}
		var j machineJoin
		if rErr = json.Unmarshal(data, &j); rErr != nil || j.JoinID == "" {
			continue
		}
		joins = append(joins, &j)
	}
	slices.SortFunc(joins, func(a, b *machineJoin) int {
		return b.UpdatedAt.Compare(a.UpdatedAt)
	})
	return joins, nil
}

// findResumable returns the most recent non-recorded attempt in the context that targets a machine with
// the given public key.
func (s *joinStore) findResumable(contextName string, publicKey []byte) *machineJoin {
	joins, err := s.list()
	if err != nil {
		return nil
	}
	for _, j := range joins {
		if j.ContextName == contextName &&
			j.Phase != joinPhaseRecorded &&
			bytes.Equal(j.PublicKey, publicKey) {
			return j
		}
	}
	return nil
}

// removeForMachine deletes persisted attempts that registered the given machine, if any. Used when a run
// discovers the machine has already joined (e.g. a previous attempt died while saving the local context).
func (s *joinStore) removeForMachine(contextName string, machineID string, publicKey []byte) error {
	joins, err := s.list()
	if err != nil {
		return err
	}
	for _, j := range joins {
		if j.ContextName != contextName {
			continue
		}
		if (j.Machine != nil && j.Machine.Id == machineID) || bytes.Equal(j.PublicKey, publicKey) {
			if rErr := s.remove(j.JoinID); rErr != nil {
				return rErr
			}
		}
	}
	return nil
}
