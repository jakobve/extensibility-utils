package flux

import (
	"context"
	"errors"
	"testing"
	"time"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/openmcp-project/extensibility-utils/pkg/objectmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func fluxTestCluster(t *testing.T) objectmanager.Cluster {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, sourcev1.AddToScheme(scheme))
	require.NoError(t, helmv2.AddToScheme(scheme))
	return objectmanager.NewCluster(fake.NewClientBuilder().WithScheme(scheme).Build(), "flux-system", objectmanager.PlatformCluster)
}

func validConfig(t *testing.T) ResourceConfig {
	t.Helper()
	return ResourceConfig{
		Cluster: fluxTestCluster(t), Namespace: "tenant", Interval: time.Hour,
		KubeConfig:    &meta.KubeConfigReference{SecretRef: &meta.SecretKeyReference{Name: "mcp-kubeconfig", Key: "kubeconfig"}},
		Version:       NewResourceVersion("1.0.0", "oci://registry.example.com/chart", "pull-secret", nil),
		OCIRepository: OCIRepositoryOptions{Name: "chart"}, HelmRelease: HelmReleaseOptions{Name: "release"},
	}
}

func TestManageResources(t *testing.T) {
	config := validConfig(t)
	config.OCIRepository.MutateFn = func(spec *sourcev1.OCIRepositorySpec) error {
		spec.Suspend = true
		return nil
	}
	config.HelmRelease.MutateFn = func(spec *helmv2.HelmReleaseSpec) error {
		spec.Suspend = true
		return nil
	}
	require.NoError(t, ManageResources(config))

	manager := objectmanager.NewManager("test")
	manager.AddCluster(config.Cluster)
	_, done, err := manager.Apply(context.Background())
	require.NoError(t, err)
	assert.False(t, done)

	ociRepository := &sourcev1.OCIRepository{}
	require.NoError(t, config.Cluster.GetClient().Get(context.Background(), client.ObjectKey{Name: "chart", Namespace: "flux-system"}, ociRepository))
	assert.Equal(t, "oci://registry.example.com/chart", ociRepository.Spec.URL)
	assert.True(t, ociRepository.Spec.Suspend)
	assert.Equal(t, "pull-secret", ociRepository.Spec.SecretRef.Name)

	helmRelease := &helmv2.HelmRelease{}
	require.NoError(t, config.Cluster.GetClient().Get(context.Background(), client.ObjectKey{Name: "release", Namespace: "flux-system"}, helmRelease))
	assert.Equal(t, "chart", helmRelease.Spec.ChartRef.Name)
	assert.Equal(t, "tenant", helmRelease.Spec.TargetNamespace)
	assert.Equal(t, "mcp-kubeconfig", helmRelease.Spec.KubeConfig.SecretRef.Name)
	assert.True(t, helmRelease.Spec.Suspend)
}

func TestManageResourcesValidation(t *testing.T) {
	config := validConfig(t)
	config.KubeConfig = nil
	assert.ErrorContains(t, ManageResources(config), "KubeConfig")
}

func TestManageResourcesReportsMutatorError(t *testing.T) {
	config := validConfig(t)
	config.OCIRepository.MutateFn = func(*sourcev1.OCIRepositorySpec) error { return errors.New("invalid override") }
	require.NoError(t, ManageResources(config))
	manager := objectmanager.NewManager("test")
	manager.AddCluster(config.Cluster)
	_, _, err := manager.Apply(context.Background())
	require.ErrorIs(t, err, objectmanager.ErrManagedObjectsFailed)
	require.ErrorContains(t, err, "invalid override")
}
