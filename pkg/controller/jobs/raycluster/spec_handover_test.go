/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package raycluster

import (
	"errors"
	"reflect"
	"testing"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/controller/jobs/ray"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	testingray "sigs.k8s.io/kueue/pkg/util/testingjobs/raycluster"
	"sigs.k8s.io/kueue/pkg/workload"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

// Exercise construction and the complete SyncJob operation, including waiting
// for deletion and recreating the remote without reflecting its stale counts.
func TestSpecDerivedSliceHandover(t *testing.T) {
	for name, tc := range map[string]struct {
		runtime, malformed, unknown, sameSlice, matching, suspended, hold, disabled, nonElastic bool
		wantDelete                                                                              bool
		foreign, unreserved, finished, evicted                                                  bool
		wantError                                                                               error
	}{
		"foreign remote is preserved":                         {foreign: true, wantError: jobframework.ErrRemoteObjectNotOwnedByMultiKueue},
		"finished slice cannot delete a successor":            {finished: true},
		"evicted slice cannot delete a successor":             {evicted: true},
		"unreserved slice cannot delete a successor":          {unreserved: true},
		"spec fallback replaces stale remote":                 {wantDelete: true},
		"suspended manager still replaces stale remote":       {suspended: true, wantDelete: true},
		"wait for deletion before creating replacement":       {hold: true, wantDelete: true},
		"malformed runtime input falls back to spec":          {malformed: true, wantDelete: true},
		"normal runtime scale-up keeps remote":                {runtime: true},
		"unknown legacy source keeps remote":                  {unknown: true},
		"autoscaler resize within current slice keeps remote": {sameSlice: true},
		"matching spec handover keeps remote":                 {matching: true},
		"autoscaling feature disabled":                        {disabled: true},
		"job not opted into elastic slicing":                  {nonElastic: true},
	} {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)
			features.SetFeatureGateDuringTest(t, features.MultiKueueRayInTreeAutoscaling, !tc.disabled)
			ctx, _ := utiltesting.ContextWithLog(t)
			local := testingray.MakeCluster("ray", "ns").ManagedBy(kueue.MultiKueueControllerName).
				WithEnableAutoscaling(new(true)).FirstWorkerGroupReplicas(0, 0, 18).
				SetAnnotation(workloadslicing.EnabledAnnotationKey, "true").Suspend(tc.suspended).Obj()
			if tc.nonElastic {
				local.Annotations[workloadslicing.EnabledAnnotationKey] = "false"
			}
			if tc.runtime {
				local.Annotations[RayClusterPodsetReplicaSizesAnnotation] = `[{"name":"workers-group-0","count":18}]`
			}
			if tc.malformed {
				local.Annotations[RayClusterPodsetReplicaSizesAnnotation] = "malformed"
			}
			manager := utiltesting.NewClientBuilder(rayv1.AddToScheme).WithObjects(local).WithStatusSubresource(local).Build()
			wl, err := jobframework.ConstructWorkload(ctx, manager, (*RayCluster)(local), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.unknown {
				delete(wl.Annotations, RayClusterPodsetReplicaSizesAnnotation)
			}
			wl.Status.Admission = &kueue.Admission{ClusterQueue: "cq"}
			wl.Status.Conditions = []metav1.Condition{{Type: kueue.WorkloadQuotaReserved, Status: metav1.ConditionTrue, Reason: "QuotaReserved"}}
			if tc.unreserved {
				wl.Status.Admission = nil
				wl.Status.Conditions = nil
			}
			if tc.finished {
				wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{Type: kueue.WorkloadFinished, Status: metav1.ConditionTrue, Reason: kueue.WorkloadSliceReplaced})
			}
			if tc.evicted {
				wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{Type: kueue.WorkloadEvicted, Status: metav1.ConditionTrue, Reason: kueue.WorkloadEvictedByPreemption})
			}
			if err := manager.Create(ctx, wl); err != nil {
				t.Fatal(err)
			}
			marker := "root"
			if tc.sameSlice {
				marker = wl.Name
			}
			replicas := int32(18)
			if tc.matching {
				replicas = 0
			}
			remote := testingray.MakeCluster(local.Name, local.Namespace).WithEnableAutoscaling(new(true)).
				FirstWorkerGroupReplicas(replicas, 0, 18).SetAnnotation(workloadslicing.EnabledAnnotationKey, "true").
				Label(kueue.MultiKueueOriginLabel, "origin").PrebuiltWorkloadLabel(marker).Suspend(false).Obj()
			remote.UID = "old-uid"
			if tc.foreign {
				remote.Labels[kueue.MultiKueueOriginLabel] = "other-origin"
			}
			if tc.hold {
				remote.Finalizers = []string{"test.example/hold"}
			}
			worker := utiltesting.NewClientBuilder(rayv1.AddToScheme).WithObjects(remote).Build()
			adapter := ray.NewMKAdapter(copyJobSpec, copyJobStatus, getEmptyList, gvk, getManagedBy, setManagedBy, ray.WithElasticReplicaSync(elasticReplicaSync()))
			before := local.DeepCopy()
			deferred, err := adapter.SyncJob(ctx, manager, worker, client.ObjectKeyFromObject(local), wl.Name, "origin")
			if !errors.Is(err, tc.wantError) {
				t.Fatalf("sync error=%v, want %v", err, tc.wantError)
			}
			actual := &rayv1.RayCluster{}
			err = worker.Get(ctx, client.ObjectKeyFromObject(remote), actual)
			if err != nil && !apierrors.IsNotFound(err) {
				t.Fatal(err)
			}
			deleting := apierrors.IsNotFound(err) || !actual.DeletionTimestamp.IsZero()
			if deleting != tc.wantDelete {
				t.Fatalf("remote deleting=%v, want %v", deleting, tc.wantDelete)
			}
			if !tc.wantDelete {
				if actual.UID != "old-uid" {
					t.Fatal("remote UID changed during a non-destructive sync")
				}
				return
			}
			if !deferred {
				t.Fatal("replacement should defer sync")
			}
			if err := manager.Get(ctx, client.ObjectKeyFromObject(local), local); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(local.Annotations, before.Annotations) {
				t.Fatal("stale runtime counts were reflected before deletion")
			}
			if tc.hold {
				if jobframework.PrebuiltWorkloadNameFor(actual) != marker {
					t.Fatal("terminating remote was repointed")
				}
				deferred, err = adapter.SyncJob(ctx, manager, worker, client.ObjectKeyFromObject(local), wl.Name, "origin")
				if err != nil || !deferred {
					t.Fatalf("pending deletion: deferred=%v err=%v", deferred, err)
				}
				if err := worker.Get(ctx, client.ObjectKeyFromObject(remote), actual); err != nil {
					t.Fatal(err)
				}
				actual.Finalizers = nil
				if err := worker.Update(ctx, actual); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := adapter.SyncJob(ctx, manager, worker, client.ObjectKeyFromObject(local), wl.Name, "origin"); err != nil {
				t.Fatal(err)
			}
			if err := worker.Get(ctx, client.ObjectKeyFromObject(remote), actual); err != nil {
				t.Fatal(err)
			}
			if actual.UID == "old-uid" {
				t.Fatal("old RayCluster was reused")
			}
			podSets, err := BuildPodSets(&actual.Spec, actual.Annotations)
			if err != nil {
				t.Fatal(err)
			}
			if !workload.ExtractPodSetCountsFromWorkload(wl).EqualTo(workload.ExtractPodSetCounts(podSets)) {
				t.Fatal("replacement did not start with spec-derived counts")
			}
			if jobframework.PrebuiltWorkloadNameFor(actual) != wl.Name {
				t.Fatal("replacement points at the wrong slice")
			}
		})
	}
}

// WorkloadAnnotations is an exported integration hook; verify both its source
// snapshot and feature-gated behavior without mutating the RayCluster.
func TestWorkloadAnnotations(t *testing.T) {
	for name, tc := range map[string]struct {
		runtime                          string
		disabled, worker, nonAutoscaling bool
		want                             map[string]string
	}{
		"spec":            {want: map[string]string{RayClusterPodsetReplicaSizesAnnotation: ""}},
		"runtime":         {runtime: `[{"name":"workers-group-0","count":18}]`, want: map[string]string{RayClusterPodsetReplicaSizesAnnotation: `[{"name":"workers-group-0","count":18}]`}},
		"malformed":       {runtime: "invalid", want: map[string]string{RayClusterPodsetReplicaSizesAnnotation: ""}},
		"disabled":        {disabled: true},
		"worker":          {worker: true},
		"non-autoscaling": {nonAutoscaling: true},
	} {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)
			features.SetFeatureGateDuringTest(t, features.MultiKueueRayInTreeAutoscaling, !tc.disabled)
			rc := testingray.MakeCluster("ray", "ns").ManagedBy(kueue.MultiKueueControllerName).WithEnableAutoscaling(new(!tc.nonAutoscaling)).
				SetAnnotation(workloadslicing.EnabledAnnotationKey, "true").SetAnnotation(RayClusterPodsetReplicaSizesAnnotation, tc.runtime).Obj()
			if tc.worker {
				rc.Spec.ManagedBy = nil
			}
			got := (*RayCluster)(rc).WorkloadAnnotations()
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("annotations=%v, want %v", got, tc.want)
			}
		})
	}
}
