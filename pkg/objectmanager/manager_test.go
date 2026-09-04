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
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testCluster(t *testing.T, objects ...runtime.Object) Cluster {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	return NewCluster(fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build(), &rest.Config{}, "default", PlatformCluster)
}

func TestManagerApplyAndDelete(t *testing.T) {
	cluster := testCluster(t)
	cluster.AddObject(NewObject(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "managed", Namespace: "default"}}, ObjectConfig{StatusFunc: SimpleStatus}))
	manager := NewManager("test")
	manager.AddCluster(cluster)

	objects, done, err := manager.Apply(context.Background())
	require.NoError(t, err)
	assert.False(t, done)
	require.Len(t, objects, 1)
	assert.Equal(t, "Secret", objects[0].Kind)
	assert.Equal(t, "managed", objects[0].Name)
	assert.Equal(t, string(PlatformCluster), objects[0].Location)

	_, done, err = manager.Delete(context.Background())
	require.NoError(t, err)
	assert.False(t, done)
	_, done, err = manager.Delete(context.Background())
	require.NoError(t, err)
	assert.True(t, done)
}

func TestManagedObjectJSON(t *testing.T) {
	encoded, err := json.Marshal(ManagedObject{APIGroup: "apps", Kind: "Deployment", Name: "app", Status: ManagedObjectStatus{Phase: StatusPhaseReady}})
	require.NoError(t, err)
	assert.JSONEq(t, `{"apiGroup":"apps","kind":"Deployment","name":"app","status":{"phase":"Ready"}}`, string(encoded))
}
