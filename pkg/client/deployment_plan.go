package client

import (
	"context"
	"fmt"

	"github.com/psviderski/uncloud/api/pb"
	"github.com/psviderski/uncloud/pkg/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetDeploymentPlan fetches a persisted deployment plan for a service. It returns api.ErrNotFound when the plan
// doesn't exist in the cluster store.
func (cli *Client) GetDeploymentPlan(
	ctx context.Context, serviceID, planID string,
) (api.DeploymentPlanRecord, error) {
	resp, err := cli.Deployment.GetPlan(ctx, &pb.GetDeploymentPlanRequest{
		ServiceId: serviceID,
		Id:        planID,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return api.DeploymentPlanRecord{}, api.ErrNotFound
		}
		return api.DeploymentPlanRecord{}, fmt.Errorf("get deployment plan: %w", err)
	}

	return api.DeploymentPlanRecord{
		ID:        resp.Id,
		Data:      resp.Data,
		UpdatedAt: resp.UpdatedAt.AsTime(),
	}, nil
}

// PutDeploymentPlan creates or updates a persisted deployment plan for a service in the cluster store.
func (cli *Client) PutDeploymentPlan(
	ctx context.Context, serviceID, planID string, data []byte,
) error {
	if _, err := cli.Deployment.PutPlan(ctx, &pb.PutDeploymentPlanRequest{
		Plan: &pb.DeploymentPlanRecord{
			Id:        planID,
			ServiceId: serviceID,
			Data:      data,
		},
	}); err != nil {
		return fmt.Errorf("put deployment plan: %w", err)
	}
	return nil
}

// ListDeploymentPlans lists all persisted deployment plans for a service from the cluster store.
func (cli *Client) ListDeploymentPlans(ctx context.Context, serviceID string) ([]api.DeploymentPlanRecord, error) {
	resp, err := cli.Deployment.ListPlans(ctx, &pb.ListDeploymentPlansRequest{ServiceId: serviceID})
	if err != nil {
		return nil, fmt.Errorf("list deployment plans: %w", err)
	}

	plans := make([]api.DeploymentPlanRecord, 0, len(resp.Plans))
	for _, record := range resp.Plans {
		plans = append(plans, api.DeploymentPlanRecord{
			ID:        record.Id,
			Data:      record.Data,
			UpdatedAt: record.UpdatedAt.AsTime(),
		})
	}
	return plans, nil
}

// DeleteDeploymentPlan deletes a persisted deployment plan for a service from the cluster store.
func (cli *Client) DeleteDeploymentPlan(ctx context.Context, serviceID, planID string) error {
	if _, err := cli.Deployment.DeletePlan(ctx, &pb.DeleteDeploymentPlanRequest{
		ServiceId: serviceID,
		Id:        planID,
	}); err != nil {
		return fmt.Errorf("delete deployment plan: %w", err)
	}
	return nil
}

// GetServiceDeploymentGeneration returns the current deployment generation (fencing token) of a service.
// A service without any deployments yet has generation 0.
func (cli *Client) GetServiceDeploymentGeneration(ctx context.Context, serviceID string) (uint64, error) {
	resp, err := cli.Deployment.GetServiceState(ctx, &pb.GetDeploymentServiceStateRequest{
		ServiceId: serviceID,
	})
	if err != nil {
		return 0, fmt.Errorf("get service deployment generation: %w", err)
	}
	return resp.Generation, nil
}

// maxGenerationAdvanceAttempts bounds the compare-and-swap retries when allocating a new generation.
const maxGenerationAdvanceAttempts = 5

// AdvanceServiceDeploymentGeneration allocates the next deployment generation (fencing token) for a service.
// It performs a compare-and-swap loop against the cluster store so concurrent callers can't allocate the same
// generation. Callers performing a deployment are expected to hold the distributed per-service deployment lease
// while calling this.
func (cli *Client) AdvanceServiceDeploymentGeneration(ctx context.Context, serviceID string) (uint64, error) {
	expected, err := cli.GetServiceDeploymentGeneration(ctx, serviceID)
	if err != nil {
		return 0, err
	}

	for attempt := 0; attempt < maxGenerationAdvanceAttempts; attempt++ {
		resp, err := cli.Deployment.AdvanceServiceState(ctx, &pb.AdvanceDeploymentServiceStateRequest{
			ServiceId:          serviceID,
			ExpectedGeneration: expected,
		})
		if err != nil {
			return 0, fmt.Errorf("advance service deployment generation: %w", err)
		}
		if resp.Generation > expected {
			// The advance was applied.
			return resp.Generation, nil
		}
		// Another caller advanced the generation first: retry with the returned current generation.
		expected = resp.Generation
	}

	return 0, fmt.Errorf("advance service deployment generation: compare-and-swap contention after %d attempts",
		maxGenerationAdvanceAttempts)
}
