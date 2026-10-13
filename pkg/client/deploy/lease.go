package deploy

// deploymentLeaseResourcePrefix scopes distributed deployment leases to one service.
const deploymentLeaseResourcePrefix = "uncloud/service/"

// DeploymentLeaseResource returns the distributed lock resource that serializes deployment planning and
// generation fencing for a service. A deployment, a scaling operation, and a service stop or removal all acquire
// this lease so they cannot allocate the same service generation concurrently.
func DeploymentLeaseResource(serviceID string) string {
	return deploymentLeaseResourcePrefix + serviceID + "/deployment"
}
