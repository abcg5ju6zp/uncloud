package api

import (
	"context"
	"strconv"
	"time"
)

const (
	// LabelDeploymentID marks a service container (including hook containers) with the stable ID of the
	// deployment plan that created it. A resuming plan uses it to find containers it created when the creation
	// response was lost, and arbitration uses it to distinguish containers owned by different deployments.
	LabelDeploymentID = "uncloud.deployment.id"
	// LabelDeploymentGeneration marks a service container with the service deployment generation (fencing token)
	// of the plan that created it. A plan must never stop or remove a container whose generation is newer than
	// the plan's own generation.
	LabelDeploymentGeneration = "uncloud.deployment.generation"
)

// DeploymentID returns the ID of the deployment plan that created the container, or an empty string when the
// container was created without deployment tracking.
func (c *ServiceContainer) DeploymentID() string {
	return c.Config.Labels[LabelDeploymentID]
}

// DeploymentGeneration returns the service deployment generation associated with the container. It returns 0 for
// containers created without a generation label (e.g. by older Uncloud versions) or when the label value is invalid.
// Such containers are treated as belonging to every earlier generation for arbitration purposes.
func (c *ServiceContainer) DeploymentGeneration() uint64 {
	raw := c.Config.Labels[LabelDeploymentGeneration]
	if raw == "" {
		return 0
	}
	gen, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return gen
}

// DeploymentIdentity identifies the deployment plan that creates a container. It is passed through the Go context
// from the durable plan executor to container creation calls, which translate it into gRPC request fields and
// container labels on the machine.
type DeploymentIdentity struct {
	// PlanID is the stable ID of the deployment plan.
	PlanID string
	// Generation is the service deployment generation (fencing token) of the plan.
	Generation uint64
	// ContainerName is an optional deterministic container name. When set, container creation uses it instead of
	// generating a random name so that a lost creation response can be reconciled by looking the container up by
	// its name.
	ContainerName string
}

// DeploymentPlanRecord is an opaque serialized deployment plan record persisted in the cluster store.
// The bytes are managed by the deploy package (JSON), API clients only transport them.
type DeploymentPlanRecord struct {
	// ID is the stable deployment plan ID.
	ID string
	// Data is the serialized deployment plan.
	Data []byte
	// UpdatedAt is the last time the record was written to the store.
	UpdatedAt time.Time
}

type deploymentIdentityKey struct{}

// WithDeploymentIdentity returns a context carrying the deployment identity used when creating containers.
func WithDeploymentIdentity(ctx context.Context, identity DeploymentIdentity) context.Context {
	return context.WithValue(ctx, deploymentIdentityKey{}, identity)
}

// DeploymentIdentityFromContext returns the deployment identity associated with the context, if any.
func DeploymentIdentityFromContext(ctx context.Context) (DeploymentIdentity, bool) {
	identity, ok := ctx.Value(deploymentIdentityKey{}).(DeploymentIdentity)
	return identity, ok
}
