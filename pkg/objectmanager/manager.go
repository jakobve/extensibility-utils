package objectmanager

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/openmcp-project/extensibility-utils/pkg/internal"
)

const (
	OperationResultDeletionFailed    controllerutil.OperationResult = "deletionFailed"
	OperationResultDeletionRequested controllerutil.OperationResult = "deletionRequested"
	OperationResultDeleted           controllerutil.OperationResult = "deleted"
	OperationResultOrphaned          controllerutil.OperationResult = OperationResultDeleted
)

// ErrManagedObjectsFailed is returned when one or more objects fail reconciliation.
var ErrManagedObjectsFailed = errors.New("one or more managed objects failed")

type dependents map[Object][]dependency

// Manager reconciles objects across one or more clusters.
type Manager interface {
	AddCluster(Cluster)
	AddCleaner(Cleaner)
	Apply(context.Context) (objects []ManagedObject, done bool, err error)
	Delete(context.Context) (objects []ManagedObject, done bool, err error)
}

type manager struct {
	serviceProvider string
	clusters        []Cluster
	cleaners        []Cleaner
}

// NewManager creates a Manager for a service provider.
func NewManager(serviceProvider string) Manager {
	return &manager{serviceProvider: serviceProvider}
}

func (m *manager) AddCluster(cluster Cluster) { m.clusters = append(m.clusters, cluster) }

func (m *manager) AddCleaner(cleaner Cleaner) { m.cleaners = append(m.cleaners, cleaner) }

func (m *manager) Apply(ctx context.Context) ([]ManagedObject, bool, error) {
	results, err := m.reconcileObjects(ctx, false)
	if err != nil {
		return nil, false, err
	}
	managedObjects, errs := resultsToManagedObjects(ctx, results)
	if len(errs) > 0 {
		return managedObjects, false, fmt.Errorf("%w: %w", ErrManagedObjectsFailed, errors.Join(errs...))
	}
	return managedObjects, allObjectsReady(results), nil
}

func (m *manager) Delete(ctx context.Context) ([]ManagedObject, bool, error) {
	results, err := m.reconcileObjects(ctx, true)
	if err != nil {
		return nil, false, err
	}
	managedObjects, errs := resultsToManagedObjects(ctx, results)
	if len(errs) > 0 {
		return managedObjects, false, fmt.Errorf("%w: %w", ErrManagedObjectsFailed, errors.Join(errs...))
	}
	return managedObjects, allDeleted(results), nil
}

func (m *manager) reconcileObjects(ctx context.Context, deleting bool) ([]Result, error) {
	dependents := m.getDependents()
	results := []Result{}
	for _, cluster := range m.clusters {
		for _, object := range cluster.GetObjects() {
			results = append(results, m.reconcileObject(ctx, cluster, object, dependents, deleting))
		}
	}
	for _, cleaner := range m.cleaners {
		resultsToAdd, err := cleaner.Cleanup(ctx)
		if err != nil {
			return results, err
		}
		results = append(results, resultsToAdd...)
	}
	if len(results) == 0 {
		log.FromContext(ctx).V(1).Info("object manager reconciled zero objects")
	}
	return results, nil
}

func (m *manager) reconcileObject(ctx context.Context, cluster Cluster, object Object, dependents dependents, deleting bool) Result {
	if deleting {
		if err := m.checkForDependents(ctx, dependents[object]); err != nil {
			return Result{Object: object, Cluster: cluster, Error: err}
		}
		if object.GetDeletionPolicy() == Orphan {
			return Result{Object: object, Cluster: cluster, OperationResult: OperationResultOrphaned}
		}
		err := cluster.GetClient().Delete(ctx, object.GetObject())
		if apierrors.IsNotFound(err) {
			return Result{Object: object, Cluster: cluster, OperationResult: OperationResultDeleted}
		}
		if err != nil {
			return Result{Object: object, Cluster: cluster, OperationResult: OperationResultDeletionFailed, Error: err}
		}
		return Result{Object: object, Cluster: cluster, OperationResult: OperationResultDeletionRequested}
	}

	result, err := controllerutil.CreateOrUpdate(ctx, cluster.GetClient(), object.GetObject(), func() error {
		internal.SetManagedBy(object.GetObject(), m.serviceProvider)
		return object.Reconcile(ctx)
	})
	return Result{Object: object, Cluster: cluster, OperationResult: result, Error: err}
}

func (m *manager) checkForDependents(ctx context.Context, dependencies []dependency) error {
	var errs []error
	for _, dependency := range dependencies {
		object := dependency.Object.GetObject()
		err := dependency.Cluster.GetClient().Get(ctx, client.ObjectKeyFromObject(object), object)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		errs = append(errs, fmt.Errorf("dependent object still exists: %s", internal.ObjectID(object)))
	}
	return errors.Join(errs...)
}

func (m *manager) getDependents() dependents {
	dependents := dependents{}
	for _, cluster := range m.clusters {
		for _, object := range cluster.GetObjects() {
			for _, dependencyObject := range object.GetDependencies() {
				dependents[dependencyObject] = append(dependents[dependencyObject], dependency{Object: object, Cluster: cluster})
			}
		}
	}
	return dependents
}

// Result summarizes one object reconciliation result.
type Result struct {
	Object          Object
	Cluster         Cluster
	OperationResult controllerutil.OperationResult
	Error           error
}

type dependency struct {
	Object  Object
	Cluster Cluster
}

func allDeleted(results []Result) bool {
	for _, result := range results {
		if result.OperationResult != OperationResultDeleted && result.OperationResult != OperationResultOrphaned {
			return false
		}
	}
	return true
}

func allObjectsReady(results []Result) bool {
	for _, result := range results {
		if result.Object.GetStatus().Phase != StatusPhaseReady {
			return false
		}
	}
	return true
}

func resultsToManagedObjects(ctx context.Context, results []Result) ([]ManagedObject, []error) {
	logger := log.FromContext(ctx)
	managedObjects := make([]ManagedObject, 0, len(results))
	var errs []error
	for _, result := range results {
		clientObject := result.Object.GetObject()
		apiGroup := ""
		kind := reflect.TypeOf(clientObject).Elem().Name()
		if gvk, err := result.Cluster.GetClient().GroupVersionKindFor(clientObject); err == nil {
			apiGroup = gvk.Group
			kind = gvk.Kind
		} else {
			logger.Error(err, "cannot determine GVK for managed object", "objectID", internal.ObjectID(clientObject))
		}
		managedObjects = append(managedObjects, ManagedObject{
			APIGroup:  apiGroup,
			Kind:      kind,
			Name:      clientObject.GetName(),
			Namespace: clientObject.GetNamespace(),
			Location:  string(result.Cluster.GetClusterType()),
			Status:    result.Object.GetStatus(),
		})
		if result.Error != nil {
			logger.Error(result.Error, "reconcile error", "objectID", internal.ObjectID(clientObject))
			errs = append(errs, result.Error)
		}
	}
	return managedObjects, errs
}
