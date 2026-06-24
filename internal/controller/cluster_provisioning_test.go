/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
*/

package controller

import (
	"context"
	"slices"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/postgres"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/specs"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func createProvisioningJob(ctx context.Context, c client.Client, cluster *apiv1.Cluster) *batchv1.Job {
	job := specs.CreatePrimaryJobViaInitdb(*cluster, 1)
	cluster.SetInheritedDataAndOwnership(&job.ObjectMeta)
	Expect(c.Create(ctx, job)).To(Succeed())
	return job
}

func markJobFailed(ctx context.Context, c client.Client, job *batchv1.Job) {
	Expect(c.Get(ctx, client.ObjectKeyFromObject(job), job)).To(Succeed())
	patch := client.MergeFrom(job.DeepCopy())
	job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
		Type:   batchv1.JobFailed,
		Status: corev1.ConditionTrue,
		Reason: "BackoffLimitExceeded",
	})
	Expect(c.Status().Patch(ctx, job, patch)).To(Succeed())
}

func markJobStuck(ctx context.Context, c client.Client, job *batchv1.Job) {
	Expect(c.Get(ctx, client.ObjectKeyFromObject(job), job)).To(Succeed())
	job.CreationTimestamp = metav1.NewTime(time.Now().Add(-10 * time.Minute))
	job.Status.StartTime = nil
	job.Status.Active = 0
	Expect(c.Update(ctx, job)).To(Succeed())
}

func markJobActive(ctx context.Context, c client.Client, job *batchv1.Job) {
	Expect(c.Get(ctx, client.ObjectKeyFromObject(job), job)).To(Succeed())
	patch := client.MergeFrom(job.DeepCopy())
	job.Status.Active = 1
	Expect(c.Status().Patch(ctx, job, patch)).To(Succeed())
}

func markJobComplete(ctx context.Context, c client.Client, job *batchv1.Job) {
	Expect(c.Get(ctx, client.ObjectKeyFromObject(job), job)).To(Succeed())
	patch := client.MergeFrom(job.DeepCopy())
	job.Status.Succeeded = 1
	// JobComplete and JobFailed are mutually exclusive: a completing Job clears
	// any prior failed condition, as the real Job controller does.
	job.Status.Conditions = slices.DeleteFunc(job.Status.Conditions,
		func(c batchv1.JobCondition) bool { return c.Type == batchv1.JobFailed })
	Expect(c.Status().Patch(ctx, job, patch)).To(Succeed())
}

func emitFailedCreateEvent(ctx context.Context, c client.Client, job *batchv1.Job, message string, lastSeen time.Time) {
	Expect(c.Get(ctx, client.ObjectKeyFromObject(job), job)).To(Succeed())
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.Name + "-failedcreate-" + lastSeen.Format("150405.000000000"),
			Namespace: job.Namespace,
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Job",
			Name:      job.Name,
			Namespace: job.Namespace,
			UID:       job.UID,
		},
		Reason:        "FailedCreate",
		Message:       message,
		Type:          "Warning",
		LastTimestamp: metav1.NewTime(lastSeen),
	}
	Expect(c.Create(ctx, event)).To(Succeed())
}

func createUnschedulablePodForJob(
	ctx context.Context,
	c client.Client,
	cluster *apiv1.Cluster,
	job *batchv1.Job,
	message string,
) {
	Expect(c.Get(ctx, client.ObjectKeyFromObject(job), job)).To(Succeed())
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.Name + "-pod",
			Namespace: job.Namespace,
			Labels: map[string]string{
				batchv1.JobNameLabel:   job.Name,
				utils.ClusterLabelName: cluster.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "batch/v1",
					Kind:       "Job",
					Name:       job.Name,
					UID:        job.UID,
					Controller: ptr.To(true),
				},
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "c", Image: "img"}},
		},
	}
	Expect(c.Create(ctx, pod)).To(Succeed())

	patch := client.MergeFrom(pod.DeepCopy())
	pod.Status.Phase = corev1.PodPending
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:    corev1.PodScheduled,
		Status:  corev1.ConditionFalse,
		Reason:  corev1.PodReasonUnschedulable,
		Message: message,
	}}
	Expect(c.Status().Patch(ctx, pod, patch)).To(Succeed())
}

// markPodScheduled marks the Pod created by createUnschedulablePodForJob as
// scheduled (PodScheduled=True), simulating a Pod that recovers once capacity
// becomes available.
func markPodScheduled(ctx context.Context, c client.Client, job *batchv1.Job) {
	pod := &corev1.Pod{}
	Expect(c.Get(ctx, types.NamespacedName{Name: job.Name + "-pod", Namespace: job.Namespace}, pod)).To(Succeed())
	patch := client.MergeFrom(pod.DeepCopy())
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:   corev1.PodScheduled,
		Status: corev1.ConditionTrue,
	}}
	Expect(c.Status().Patch(ctx, pod, patch)).To(Succeed())
}

func getProvisioningCondition(cluster *apiv1.Cluster) *metav1.Condition {
	return meta.FindStatusCondition(cluster.Status.Conditions, string(apiv1.ConditionProvisioning))
}

func reconcileProvisioningResources(
	ctx context.Context,
	r *ClusterReconciler,
	cluster *apiv1.Cluster,
) (time.Duration, error) {
	resources, err := r.getManagedResources(ctx, cluster)
	if err != nil {
		return 0, err
	}
	result, err := r.reconcileResources(ctx, cluster, resources, postgres.PostgresqlStatusList{})
	if err != nil {
		return 0, err
	}
	return result.RequeueAfter, nil
}

func getRemoteCluster(ctx context.Context, c client.Client, cluster *apiv1.Cluster) *apiv1.Cluster {
	remote := &apiv1.Cluster{}
	Expect(c.Get(ctx, types.NamespacedName{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
	}, remote)).To(Succeed())
	return remote
}

var _ = Describe("Provisioning job failure integration", func() {
	var (
		env     *testingEnvironment
		cluster *apiv1.Cluster
		ctx     context.Context
	)

	BeforeEach(func() {
		env = buildTestEnvironment()
		ctx = context.Background()
		namespace := newFakeNamespace(env.client)
		cluster = newFakeCNPGCluster(env.client, namespace, func(c *apiv1.Cluster) {
			c.Spec.Instances = 1
		})
	})

	It("surfaces a failed provisioning job on cluster status", func() {
		job := createProvisioningJob(ctx, env.client, cluster)
		markJobFailed(ctx, env.client, job)

		requeueAfter, err := reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())
		Expect(requeueAfter).To(Equal(5 * time.Second))

		condition := getProvisioningCondition(cluster)
		Expect(condition).ToNot(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Reason).To(Equal(string(apiv1.ProvisioningJobFailed)))
		Expect(condition.Message).To(ContainSubstring(job.Name))

		remote := getRemoteCluster(ctx, env.client, cluster)
		remoteCondition := getProvisioningCondition(remote)
		Expect(remoteCondition).ToNot(BeNil())
		Expect(remoteCondition.Status).To(Equal(metav1.ConditionFalse))
		Expect(remoteCondition.Reason).To(Equal(string(apiv1.ProvisioningJobFailed)))
		Expect(remoteCondition.Message).To(ContainSubstring(job.Name))

		fakeRecorder, ok := env.clusterReconciler.Recorder.(*record.FakeRecorder)
		Expect(ok).To(BeTrue())
		var recorded string
		Eventually(fakeRecorder.Events, "1s").Should(Receive(&recorded))
		Expect(recorded).To(HavePrefix("Warning ProvisioningJobFailed"))
		Expect(recorded).To(ContainSubstring(job.Name))
	})

	It("surfaces a stuck provisioning job on cluster status", func() {
		job := createProvisioningJob(ctx, env.client, cluster)
		markJobStuck(ctx, env.client, job)

		requeueAfter, err := reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())
		Expect(requeueAfter).To(Equal(5 * time.Second))

		condition := getProvisioningCondition(cluster)
		Expect(condition).ToNot(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Reason).To(Equal(string(apiv1.ProvisioningJobStuck)))
		Expect(condition.Message).To(ContainSubstring("ResourceQuota"))
		Expect(condition.Message).To(ContainSubstring(job.Name))

		// No identifiable root cause: the phase is not moved to unschedulable.
		Expect(cluster.Status.Phase).ToNot(Equal(apiv1.PhaseUnschedulable))

		fakeRecorder, ok := env.clusterReconciler.Recorder.(*record.FakeRecorder)
		Expect(ok).To(BeTrue())
		var recorded string
		Eventually(fakeRecorder.Events, "1s").Should(Receive(&recorded))
		Expect(recorded).To(HavePrefix("Warning ProvisioningJobStuck"))
		Expect(recorded).To(ContainSubstring(job.Name))
	})

	It("enriches a stuck job message with the FailedCreate event root cause", func() {
		job := createProvisioningJob(ctx, env.client, cluster)
		markJobStuck(ctx, env.client, job)
		emitFailedCreateEvent(ctx, env.client, job,
			`Error creating: pods "x" is forbidden: exceeded quota: compute, requested: pods=1`,
			time.Now())

		_, err := reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())

		condition := getProvisioningCondition(cluster)
		Expect(condition).ToNot(BeNil())
		Expect(condition.Reason).To(Equal(string(apiv1.ProvisioningJobStuck)))
		// The generic heuristic is preserved...
		Expect(condition.Message).To(ContainSubstring("ResourceQuota"))
		// ...and the concrete root cause from the event is appended.
		Expect(condition.Message).To(ContainSubstring("exceeded quota"))

		// An identified root cause also moves the cluster to the unschedulable phase.
		Expect(cluster.Status.Phase).To(Equal(apiv1.PhaseUnschedulable))
		Expect(cluster.Status.PhaseReason).To(ContainSubstring("exceeded quota"))
	})

	It("prefers a live unschedulable Pod reason over a stale FailedCreate event", func() {
		job := createProvisioningJob(ctx, env.client, cluster)
		markJobStuck(ctx, env.client, job)
		// A stale creation blocker from an earlier back-off attempt...
		emitFailedCreateEvent(ctx, env.client, job,
			`Error creating: pods "x" is forbidden: exceeded quota: compute`,
			time.Now().Add(-time.Hour))
		// ...and a Pod that was eventually created but cannot be scheduled now.
		createUnschedulablePodForJob(ctx, env.client, cluster, job,
			"0/3 nodes are available: insufficient cpu")

		_, err := reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())

		condition := getProvisioningCondition(cluster)
		Expect(condition).ToNot(BeNil())
		Expect(condition.Reason).To(Equal(string(apiv1.ProvisioningJobStuck)))
		// The live scheduling failure wins over the stale creation event.
		Expect(condition.Message).To(ContainSubstring("insufficient cpu"))
		Expect(condition.Message).ToNot(ContainSubstring("exceeded quota"))
	})

	It("enriches a failed job message with the FailedCreate event root cause", func() {
		job := createProvisioningJob(ctx, env.client, cluster)
		markJobFailed(ctx, env.client, job)
		emitFailedCreateEvent(ctx, env.client, job,
			`admission webhook "validate.example.com" denied the request`,
			time.Now())

		_, err := reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())

		condition := getProvisioningCondition(cluster)
		Expect(condition).ToNot(BeNil())
		Expect(condition.Reason).To(Equal(string(apiv1.ProvisioningJobFailed)))
		Expect(condition.Message).To(ContainSubstring("admission webhook"))
	})

	It("keeps the default message when no FailedCreate event is present", func() {
		job := createProvisioningJob(ctx, env.client, cluster)
		markJobStuck(ctx, env.client, job)
		// An unrelated event must not be picked up.
		event := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-sched", Namespace: job.Namespace},
			InvolvedObject: corev1.ObjectReference{
				Kind: "Job", Name: job.Name, Namespace: job.Namespace, UID: job.UID,
			},
			Reason:        "SuccessfulCreate",
			Message:       "Created pod: should-not-appear",
			LastTimestamp: metav1.NewTime(time.Now()),
		}
		Expect(env.client.Create(ctx, event)).To(Succeed())

		_, err := reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())

		condition := getProvisioningCondition(cluster)
		Expect(condition).ToNot(BeNil())
		Expect(condition.Message).To(ContainSubstring("ResourceQuota"))
		Expect(condition.Message).ToNot(ContainSubstring("should-not-appear"))
		Expect(condition.Message).ToNot(ContainSubstring("Latest blocker"))
	})

	It("reports in-progress provisioning for a healthy job", func() {
		job := createProvisioningJob(ctx, env.client, cluster)
		markJobActive(ctx, env.client, job)

		requeueAfter, err := reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())
		Expect(requeueAfter).To(Equal(5 * time.Second))

		condition := getProvisioningCondition(cluster)
		Expect(condition).ToNot(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionTrue))
		Expect(condition.Reason).To(Equal(string(apiv1.ProvisioningHealthy)))
		Expect(condition.Message).To(Equal("Provisioning is in progress"))
	})

	It("clears a stale provisioning problem when the job completes", func() {
		job := createProvisioningJob(ctx, env.client, cluster)
		markJobFailed(ctx, env.client, job)

		_, err := reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())
		Expect(getProvisioningCondition(cluster).Status).To(Equal(metav1.ConditionFalse))

		markJobComplete(ctx, env.client, job)

		_, err = reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())

		condition := getProvisioningCondition(cluster)
		Expect(condition).ToNot(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionTrue))
		Expect(condition.Reason).To(Equal(string(apiv1.ProvisioningIdle)))
		Expect(condition.Message).To(Equal("No provisioning in progress"))
	})

	It("does not churn the provisioning condition on repeated reconcile", func() {
		job := createProvisioningJob(ctx, env.client, cluster)
		markJobFailed(ctx, env.client, job)

		_, err := reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())

		conditionAfterFirst := getProvisioningCondition(cluster).DeepCopy()
		Expect(conditionAfterFirst).ToNot(BeNil())

		_, err = reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())

		conditionAfterSecond := getProvisioningCondition(cluster)
		Expect(conditionAfterSecond).ToNot(BeNil())
		Expect(conditionAfterSecond.Status).To(Equal(conditionAfterFirst.Status))
		Expect(conditionAfterSecond.Reason).To(Equal(conditionAfterFirst.Reason))
		Expect(conditionAfterSecond.Message).To(Equal(conditionAfterFirst.Message))
		Expect(conditionAfterSecond.LastTransitionTime).To(Equal(conditionAfterFirst.LastTransitionTime))
	})

	It("escalates from stuck to unrecoverable when a stuck job later fails", func() {
		job := createProvisioningJob(ctx, env.client, cluster)
		markJobStuck(ctx, env.client, job)
		emitFailedCreateEvent(ctx, env.client, job,
			`Error creating: exceeded quota: compute`, time.Now())

		_, err := reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())
		Expect(cluster.Status.Phase).To(Equal(apiv1.PhaseUnschedulable))
		Expect(getProvisioningCondition(cluster).Reason).To(Equal(string(apiv1.ProvisioningJobStuck)))

		markJobFailed(ctx, env.client, job)

		_, err = reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())
		Expect(cluster.Status.Phase).To(Equal(apiv1.PhaseUnrecoverable))

		condition := getProvisioningCondition(cluster)
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Reason).To(Equal(string(apiv1.ProvisioningJobFailed)))
	})

	It("clears the unschedulable phase once the Pod becomes schedulable", func() {
		job := createProvisioningJob(ctx, env.client, cluster)
		markJobStuck(ctx, env.client, job)
		createUnschedulablePodForJob(ctx, env.client, cluster, job,
			"0/3 nodes are available: insufficient cpu")

		_, err := reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())
		Expect(cluster.Status.Phase).To(Equal(apiv1.PhaseUnschedulable))

		// The Pod schedules and the Job starts making progress.
		markPodScheduled(ctx, env.client, job)
		markJobActive(ctx, env.client, job)

		_, err = reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())
		Expect(cluster.Status.Phase).ToNot(Equal(apiv1.PhaseUnschedulable))

		condition := getProvisioningCondition(cluster)
		Expect(condition.Status).To(Equal(metav1.ConditionTrue))
		Expect(condition.Reason).To(Equal(string(apiv1.ProvisioningHealthy)))
	})

	It("sets the unschedulable phase only once a root cause is identified", func() {
		job := createProvisioningJob(ctx, env.client, cluster)
		markJobStuck(ctx, env.client, job)

		// No event nor Pod yet: stuck condition, but no phase.
		_, err := reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())
		Expect(getProvisioningCondition(cluster).Reason).To(Equal(string(apiv1.ProvisioningJobStuck)))
		Expect(cluster.Status.Phase).ToNot(Equal(apiv1.PhaseUnschedulable))

		// A FailedCreate event appears: now the phase is set.
		emitFailedCreateEvent(ctx, env.client, job,
			`Error creating: exceeded quota: compute`, time.Now())

		_, err = reconcileProvisioningResources(ctx, env.clusterReconciler, cluster)
		Expect(err).ToNot(HaveOccurred())
		Expect(cluster.Status.Phase).To(Equal(apiv1.PhaseUnschedulable))
		Expect(cluster.Status.PhaseReason).To(ContainSubstring("exceeded quota"))
	})
})
