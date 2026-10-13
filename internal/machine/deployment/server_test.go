package deployment

import (
	"context"
	"testing"
	"time"

	"github.com/psviderski/uncloud/api/pb"
	"github.com/psviderski/uncloud/internal/machine/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testServiceID = "0123456789abcdef0123456789abcdef"
	testPlanID    = "fedcba9876543210fedcba9876543210"
)

// fakeKeyspace is an in-memory implementation of the deployment keyspace for server tests.
type fakeKeyspace struct {
	records map[string][]byte
}

func newFakeKeyspace() *fakeKeyspace {
	return &fakeKeyspace{records: make(map[string][]byte)}
}

func (k *fakeKeyspace) Get(_ context.Context, key string) (store.Record, error) {
	value, ok := k.records[key]
	if !ok {
		return store.Record{}, store.ErrKeyNotFound
	}
	return store.Record{Key: key, Value: value, UpdatedAt: time.Now()}, nil
}

func (k *fakeKeyspace) Put(_ context.Context, key string, value []byte) error {
	k.records[key] = value
	return nil
}

func (k *fakeKeyspace) Delete(_ context.Context, key string, _ store.KeyspaceDeleteOptions) error {
	delete(k.records, key)
	return nil
}

func (k *fakeKeyspace) List(_ context.Context, prefix string, _ store.KeyspaceListOptions) ([]store.Record, error) {
	var records []store.Record
	for key, value := range k.records {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			records = append(records, store.Record{Key: key, Value: value})
		}
	}
	return records, nil
}

func TestServerPlanLifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	srv := NewServer(newFakeKeyspace())

	_, err := srv.GetPlan(ctx, &pb.GetDeploymentPlanRequest{ServiceId: testServiceID, Id: testPlanID})
	require.Equal(t, codes.NotFound, status.Code(err))

	_, err = srv.PutPlan(ctx, &pb.PutDeploymentPlanRequest{Plan: &pb.DeploymentPlanRecord{
		Id:        testPlanID,
		ServiceId: testServiceID,
		Data:      []byte(`{"hello":"plan"}`),
	}})
	require.NoError(t, err)

	got, err := srv.GetPlan(ctx, &pb.GetDeploymentPlanRequest{ServiceId: testServiceID, Id: testPlanID})
	require.NoError(t, err)
	assert.Equal(t, testPlanID, got.Id)
	assert.Equal(t, testServiceID, got.ServiceId)
	assert.Equal(t, []byte(`{"hello":"plan"}`), got.Data)

	list, err := srv.ListPlans(ctx, &pb.ListDeploymentPlansRequest{ServiceId: testServiceID})
	require.NoError(t, err)
	require.Len(t, list.Plans, 1)
	assert.Equal(t, testPlanID, list.Plans[0].Id)

	_, err = srv.DeletePlan(ctx, &pb.DeleteDeploymentPlanRequest{ServiceId: testServiceID, Id: testPlanID})
	require.NoError(t, err)

	list, err = srv.ListPlans(ctx, &pb.ListDeploymentPlansRequest{ServiceId: testServiceID})
	require.NoError(t, err)
	assert.Empty(t, list.Plans)
}

func TestServerPlanValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	srv := NewServer(newFakeKeyspace())

	_, err := srv.GetPlan(ctx, &pb.GetDeploymentPlanRequest{ServiceId: "bad", Id: testPlanID})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = srv.PutPlan(ctx, &pb.PutDeploymentPlanRequest{Plan: &pb.DeploymentPlanRecord{
		Id:        "bad",
		ServiceId: testServiceID,
		Data:      []byte("x"),
	}})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = srv.PutPlan(ctx, &pb.PutDeploymentPlanRequest{Plan: &pb.DeploymentPlanRecord{
		Id:        testPlanID,
		ServiceId: testServiceID,
	}})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestServerAdvanceServiceStateCAS(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	srv := NewServer(newFakeKeyspace())

	// A service without state reports generation 0.
	state, err := srv.GetServiceState(ctx, &pb.GetDeploymentServiceStateRequest{ServiceId: testServiceID})
	require.NoError(t, err)
	assert.EqualValues(t, 0, state.Generation)

	// Advance with the expected generation allocates the next one.
	state, err = srv.AdvanceServiceState(ctx, &pb.AdvanceDeploymentServiceStateRequest{
		ServiceId:          testServiceID,
		ExpectedGeneration: 0,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, state.Generation)

	// A stale expectation does not advance: the current generation is returned unchanged.
	state, err = srv.AdvanceServiceState(ctx, &pb.AdvanceDeploymentServiceStateRequest{
		ServiceId:          testServiceID,
		ExpectedGeneration: 0,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, state.Generation)

	// The correct expectation advances again.
	state, err = srv.AdvanceServiceState(ctx, &pb.AdvanceDeploymentServiceStateRequest{
		ServiceId:          testServiceID,
		ExpectedGeneration: 1,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 2, state.Generation)
}

func TestServerListPlansFiltersByService(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ks := newFakeKeyspace()
	srv := NewServer(ks)

	otherService := "11111111111111111111111111111111"
	require.NoError(t, ks.Put(ctx, planKey(testServiceID, testPlanID), []byte("one")))
	require.NoError(t, ks.Put(ctx, planKey(otherService, testPlanID), []byte("two")))
	// An unexpected key in the service prefix must be ignored instead of breaking the listing.
	require.NoError(t, ks.Put(ctx, planListPrefix(testServiceID)+"garbage", []byte("three")))

	list, err := srv.ListPlans(ctx, &pb.ListDeploymentPlansRequest{ServiceId: testServiceID})
	require.NoError(t, err)
	require.Len(t, list.Plans, 1)
	assert.Equal(t, testPlanID, list.Plans[0].Id)
	assert.Equal(t, []byte("one"), list.Plans[0].Data)
}
