package objectmanager

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func testCluster(t *testing.T, objects ...runtime.Object) Cluster {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	return NewCluster(fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build(), "default", PlatformCluster)
}

func TestManagerApplyAndDelete(t *testing.T) {
	cluster := testCluster(t)
	cluster.AddObject(NewObject(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "managed", Namespace: "default"}}, ObjectConfig{StatusFunc: SimpleStatus}))
	manager := NewManager("test")
	manager.AddCluster(cluster)

	applyResult := manager.Apply(context.Background())
	require.NoError(t, applyResult.Err)
	assert.False(t, applyResult.Done)
	require.Len(t, applyResult.ManagedObjectResults, 1)
	assert.Equal(t, "Secret", applyResult.ManagedObjectResults[0].ManagedObject.Kind)
	assert.Equal(t, "managed", applyResult.ManagedObjectResults[0].ManagedObject.Name)
	assert.Equal(t, string(PlatformCluster), applyResult.ManagedObjectResults[0].ManagedObject.Location)
	assert.Equal(t, controllerutil.OperationResultCreated, applyResult.ManagedObjectResults[0].OperationResult)

	deleteResult := manager.Delete(context.Background())
	require.NoError(t, deleteResult.Err)
	assert.False(t, deleteResult.Done)
	require.Len(t, deleteResult.ManagedObjectResults, 1)
	assert.Equal(t, OperationResultDeletionRequested, deleteResult.ManagedObjectResults[0].OperationResult)

	deleteResult = manager.Delete(context.Background())
	require.NoError(t, deleteResult.Err)
	assert.True(t, deleteResult.Done)
}

func TestManagedObjectJSON(t *testing.T) {
	encoded, err := json.Marshal(ManagedObject{APIGroup: "apps", Kind: "Deployment", Name: "app", Status: ManagedObjectStatus{Phase: StatusPhaseReady}})
	require.NoError(t, err)
	assert.JSONEq(t, `{"apiGroup":"apps","kind":"Deployment","name":"app","status":{"phase":"Ready"}}`, string(encoded))
}

func TestReconcileResultManagedObjectHelpers(t *testing.T) {
	reconcileResult := ReconcileResult{
		ManagedObjectResults: []ManagedObjectResult{
			{
				ManagedObject: ManagedObject{Name: "ready", Kind: "Secret"},
			},
			{
				ManagedObject: ManagedObject{Name: "failed", Kind: "ConfigMap"},
				Err:           assert.AnError,
			},
		},
	}

	assert.Equal(t, []ManagedObject{
		{Name: "ready", Kind: "Secret"},
		{Name: "failed", Kind: "ConfigMap"},
	}, reconcileResult.GetAllManagedObjects())

	assert.Equal(t, []ManagedObjectResult{
		{
			ManagedObject: ManagedObject{Name: "failed", Kind: "ConfigMap"},
			Err:           assert.AnError,
		},
	}, reconcileResult.GetFailedManagedObjectResults())

	assert.Equal(t, []ManagedObjectResult{
		{
			ManagedObject: ManagedObject{Name: "ready", Kind: "Secret"},
		},
	}, reconcileResult.GetSucceededManagedObjectResults())
}
