// Package deployment implements the cluster API for durable deployment plans and per-service generation
// fencing tokens. See api/pb/deployment.proto for the gRPC service definition.
//
// Records are stored in the distributed Corrosion cluster store under a dedicated "deployment" keyspace:
//
//   - svc:<service_id>                     -> serialized service state (generation fencing token)
//   - svc:<service_id>/plan/<plan_id>      -> serialized deployment plan record
//
// Writes made on any machine replicate through the eventually consistent CRDT store.
package deployment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/psviderski/uncloud/api/pb"
	"github.com/psviderski/uncloud/internal/machine/store"
	"github.com/psviderski/uncloud/pkg/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Namespace identifies deployment records in the cluster key-value store.
const Namespace = "deployment"

// Server implements the Deployment gRPC service.
type Server struct {
	pb.UnimplementedDeploymentServer

	store keyspace

	// advanceMu serializes generation compare-and-swap requests for the same service handled by this machine.
	// Cross-machine serialization is provided by the distributed deployment lease held by plan executors.
	advanceMu sync.Map // map[string]*sync.Mutex
}

type keyspace interface {
	Get(context.Context, string) (store.Record, error)
	Put(context.Context, string, []byte) error
	Delete(context.Context, string, store.KeyspaceDeleteOptions) error
	List(context.Context, string, store.KeyspaceListOptions) ([]store.Record, error)
}

func NewServer(store keyspace) *Server {
	return &Server{store: store}
}

// serviceState is the serialized value of the per-service deployment state record.
type serviceState struct {
	Generation uint64 `json:"generation"`
}

func stateKey(serviceID string) string {
	return "svc:" + serviceID
}

func planKey(serviceID, planID string) string {
	return fmt.Sprintf("svc:%s/plan/%s", serviceID, planID)
}

func planListPrefix(serviceID string) string {
	return fmt.Sprintf("svc:%s/plan/", serviceID)
}

func validateServiceID(serviceID string) error {
	if !api.ValidateServiceID(serviceID) {
		return status.Errorf(codes.InvalidArgument, "invalid service ID: %q", serviceID)
	}
	return nil
}

func validatePlanID(planID string) error {
	if !api.IDRegex.MatchString(planID) {
		return status.Errorf(codes.InvalidArgument, "invalid deployment plan ID: %q", planID)
	}
	return nil
}

// mutexFor returns the per-service mutex used to serialize local generation advances.
func (s *Server) mutexFor(serviceID string) *sync.Mutex {
	mu, _ := s.advanceMu.LoadOrStore(serviceID, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (s *Server) GetPlan(ctx context.Context, req *pb.GetDeploymentPlanRequest) (*pb.DeploymentPlanRecord, error) {
	if err := validateServiceID(req.ServiceId); err != nil {
		return nil, err
	}
	if err := validatePlanID(req.Id); err != nil {
		return nil, err
	}

	record, err := s.store.Get(ctx, planKey(req.ServiceId, req.Id))
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, status.Errorf(codes.NotFound, "deployment plan %q not found for service %q",
			req.Id, req.ServiceId)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get deployment plan: %v", err)
	}

	return &pb.DeploymentPlanRecord{
		Id:        req.Id,
		ServiceId: req.ServiceId,
		Data:      record.Value,
		UpdatedAt: timestamppb.New(record.UpdatedAt),
	}, nil
}

func (s *Server) PutPlan(ctx context.Context, req *pb.PutDeploymentPlanRequest) (*emptypb.Empty, error) {
	record := req.GetPlan()
	if record == nil {
		return nil, status.Error(codes.InvalidArgument, "plan record is required")
	}
	if err := validateServiceID(record.ServiceId); err != nil {
		return nil, err
	}
	if err := validatePlanID(record.Id); err != nil {
		return nil, err
	}
	if len(record.Data) == 0 {
		return nil, status.Error(codes.InvalidArgument, "deployment plan data is empty")
	}

	if err := s.store.Put(ctx, planKey(record.ServiceId, record.Id), record.Data); err != nil {
		return nil, status.Errorf(codes.Internal, "put deployment plan: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) ListPlans(
	ctx context.Context, req *pb.ListDeploymentPlansRequest,
) (*pb.ListDeploymentPlansResponse, error) {
	if err := validateServiceID(req.ServiceId); err != nil {
		return nil, err
	}

	records, err := s.store.List(ctx, planListPrefix(req.ServiceId), store.KeyspaceListOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list deployment plans: %v", err)
	}

	resp := &pb.ListDeploymentPlansResponse{Plans: make([]*pb.DeploymentPlanRecord, 0, len(records))}
	for _, record := range records {
		// Keys have the form svc:<service_id>/plan/<plan_id>.
		planID, ok := parsePlanID(record.Key)
		if !ok {
			// Ignore keys that don't match the expected layout instead of failing the whole listing.
			continue
		}
		resp.Plans = append(resp.Plans, &pb.DeploymentPlanRecord{
			Id:        planID,
			ServiceId: req.ServiceId,
			Data:      record.Value,
			UpdatedAt: timestamppb.New(record.UpdatedAt),
		})
	}
	return resp, nil
}

// parsePlanID extracts the plan ID suffix from a key with the svc:<service_id>/plan/<plan_id> layout.
func parsePlanID(key string) (string, bool) {
	const marker = "/plan/"
	idx := strings.LastIndex(key, marker)
	if idx < 0 {
		return "", false
	}
	planID := key[idx+len(marker):]
	if !api.IDRegex.MatchString(planID) {
		return "", false
	}
	return planID, true
}

func (s *Server) DeletePlan(ctx context.Context, req *pb.DeleteDeploymentPlanRequest) (*emptypb.Empty, error) {
	if err := validateServiceID(req.ServiceId); err != nil {
		return nil, err
	}
	if err := validatePlanID(req.Id); err != nil {
		return nil, err
	}

	if err := s.store.Delete(ctx, planKey(req.ServiceId, req.Id), store.KeyspaceDeleteOptions{}); err != nil {
		return nil, status.Errorf(codes.Internal, "delete deployment plan: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) GetServiceState(
	ctx context.Context, req *pb.GetDeploymentServiceStateRequest,
) (*pb.DeploymentServiceState, error) {
	if err := validateServiceID(req.ServiceId); err != nil {
		return nil, err
	}

	state, updatedAt, err := s.getServiceState(ctx, req.ServiceId)
	if err != nil {
		return nil, err
	}
	return &pb.DeploymentServiceState{
		ServiceId:  req.ServiceId,
		Generation: state.Generation,
		UpdatedAt:  timestamppb.New(updatedAt),
	}, nil
}

// getServiceState reads the current service state. A missing record means generation 0.
func (s *Server) getServiceState(ctx context.Context, serviceID string) (serviceState, time.Time, error) {
	record, err := s.store.Get(ctx, stateKey(serviceID))
	if errors.Is(err, store.ErrKeyNotFound) {
		return serviceState{}, time.Time{}, nil
	}
	if err != nil {
		return serviceState{}, time.Time{}, status.Errorf(codes.Internal, "get deployment service state: %v", err)
	}

	var state serviceState
	if len(record.Value) > 0 {
		if err = json.Unmarshal(record.Value, &state); err != nil {
			return serviceState{}, time.Time{},
				status.Errorf(codes.Internal, "unmarshal deployment service state: %v", err)
		}
	}
	return state, record.UpdatedAt, nil
}

func (s *Server) AdvanceServiceState(
	ctx context.Context, req *pb.AdvanceDeploymentServiceStateRequest,
) (*pb.DeploymentServiceState, error) {
	if err := validateServiceID(req.ServiceId); err != nil {
		return nil, err
	}

	// Serialize concurrent advances for the same service handled by this machine. Cross-machine callers are
	// expected to hold the distributed deployment lease, but the local mutex keeps the read-modify-write atomic
	// against goroutines sharing this daemon.
	mu := s.mutexFor(req.ServiceId)
	mu.Lock()
	defer mu.Unlock()

	state, _, err := s.getServiceState(ctx, req.ServiceId)
	if err != nil {
		return nil, err
	}

	// Compare-and-swap: advance only when the stored generation still matches the caller's expectation.
	// The current state is returned either way so the caller can retry with an up-to-date expectation.
	if state.Generation == req.ExpectedGeneration {
		state.Generation++
		data, err := json.Marshal(state)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshal deployment service state: %v", err)
		}
		if err = s.store.Put(ctx, stateKey(req.ServiceId), data); err != nil {
			return nil, status.Errorf(codes.Internal, "advance deployment service state: %v", err)
		}
	}

	return &pb.DeploymentServiceState{
		ServiceId:  req.ServiceId,
		Generation: state.Generation,
	}, nil
}
