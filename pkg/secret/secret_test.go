package secret

import (
	"context"
	"testing"

	"github.com/openmcp-project/extensibility-utils/pkg/objectmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestManagePullSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	source := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "source", Namespace: "source"}, Data: map[string][]byte{"config": []byte("value")}}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(source).Build()
	cluster := objectmanager.NewCluster(fakeClient, &rest.Config{}, "target", objectmanager.PlatformCluster)
	ManagePullSecret(cluster, CopyConfig{SourceClient: fakeClient, SourceName: "source", SourceNamespace: "source", TargetName: "target", TargetNamespace: "target"})

	manager := objectmanager.NewManager("test")
	manager.AddCluster(cluster)
	_, _, err := manager.Apply(context.Background())
	require.NoError(t, err)

	target := &corev1.Secret{}
	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKey{Name: "target", Namespace: "target"}, target))
	assert.Equal(t, source.Data, target.Data)
	assert.Equal(t, corev1.SecretTypeDockerConfigJson, target.Type)
}
