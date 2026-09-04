package objectmanager

import (
	"strings"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClusterType identifies the role of a target cluster.
type ClusterType string

const (
	ManagedControlPlane ClusterType = "ManagedControlPlane"
	PlatformCluster     ClusterType = "PlatformCluster"
	WorkloadCluster     ClusterType = "WorkloadCluster"
)

// Cluster holds objects that are reconciled against one Kubernetes client.
type Cluster interface {
	AddObject(Object)
	GetObjects() []Object
	GetDefaultNamespace() string
	GetHostAndPort() (string, string)
	GetConfig() *rest.Config
	GetClient() client.Client
	GetClusterType() ClusterType
}

type cluster struct {
	client           client.Client
	config           *rest.Config
	objects          []Object
	defaultNamespace string
	clusterType      ClusterType
}

var _ Cluster = (*cluster)(nil)

// NewCluster creates an object-manager cluster from a Kubernetes client.
func NewCluster(client client.Client, config *rest.Config, namespace string, clusterType ClusterType) Cluster {
	return &cluster{
		client:           client,
		config:           config,
		objects:          []Object{},
		defaultNamespace: namespace,
		clusterType:      clusterType,
	}
}

func (c *cluster) AddObject(object Object) { c.objects = append(c.objects, object) }

func (c *cluster) GetObjects() []Object { return c.objects }

func (c *cluster) GetDefaultNamespace() string { return c.defaultNamespace }

func (c *cluster) GetConfig() *rest.Config { return c.config }

func (c *cluster) GetClient() client.Client { return c.client }

func (c *cluster) GetClusterType() ClusterType { return c.clusterType }

func (c *cluster) GetHostAndPort() (string, string) {
	input := strings.TrimPrefix(strings.TrimPrefix(c.config.Host, "https://"), "http://")
	host, port, found := strings.Cut(input, ":")
	if !found {
		port = "443"
	}
	return host, port
}
