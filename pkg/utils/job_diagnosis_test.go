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

package utils

import (
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Job diagnosis", func() {
	now := time.Date(2026, 6, 21, 12, 0, 0, 0, time.UTC)
	const threshold = 5 * time.Minute

	newJob := func(name string, mutators ...func(*batchv1.Job)) batchv1.Job {
		job := batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				CreationTimestamp: metav1.NewTime(now),
			},
		}
		for _, m := range mutators {
			m(&job)
		}
		return job
	}

	withCreatedAgo := func(d time.Duration) func(*batchv1.Job) {
		return func(j *batchv1.Job) {
			j.CreationTimestamp = metav1.NewTime(now.Add(-d))
		}
	}
	withStartTime := func(d time.Duration) func(*batchv1.Job) {
		return func(j *batchv1.Job) {
			t := metav1.NewTime(now.Add(-d))
			j.Status.StartTime = &t
		}
	}
	withActive := func(n int32) func(*batchv1.Job) {
		return func(j *batchv1.Job) { j.Status.Active = n }
	}
	withFailedCondition := func(j *batchv1.Job) {
		j.Status.Conditions = append(j.Status.Conditions, batchv1.JobCondition{
			Type:   batchv1.JobFailed,
			Status: corev1.ConditionTrue,
			Reason: "BackoffLimitExceeded",
		})
	}

	// unschedulablePod builds a Pending Pod owned by the named job whose
	// PodScheduled condition is False/Unschedulable, created the given duration
	// ago.
	unschedulablePod := func(jobName string, createdAgo time.Duration, message string) corev1.Pod {
		return corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              jobName + "-pod",
				CreationTimestamp: metav1.NewTime(now.Add(-createdAgo)),
				Labels:            map[string]string{batchv1.JobNameLabel: jobName},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{
					{
						Type:    corev1.PodScheduled,
						Status:  corev1.ConditionFalse,
						Reason:  corev1.PodReasonUnschedulable,
						Message: message,
					},
				},
			},
		}
	}

	Describe("ClassifyJob", func() {
		It("reports a freshly created job as in progress", func() {
			job := newJob("fresh")
			Expect(ClassifyJob(job, nil, now, threshold)).To(Equal(JobHealthInProgress))
		})

		It("reports a job with an active pod as in progress, even if old", func() {
			job := newJob("active", withCreatedAgo(time.Hour), withActive(1))
			Expect(ClassifyJob(job, nil, now, threshold)).To(Equal(JobHealthInProgress))
		})

		It("reports a job that started a pod as in progress, even without start being recent", func() {
			job := newJob("started", withCreatedAgo(time.Hour), withStartTime(50*time.Minute))
			Expect(ClassifyJob(job, nil, now, threshold)).To(Equal(JobHealthInProgress))
		})

		It("reports a permanently failed job as failed", func() {
			job := newJob("failed", withFailedCondition)
			Expect(ClassifyJob(job, nil, now, threshold)).To(Equal(JobHealthFailed))
		})

		It("prefers failed over stuck when both could apply", func() {
			// Old, never started, AND carries the failed condition.
			job := newJob("failed-and-old", withCreatedAgo(time.Hour), withFailedCondition)
			Expect(ClassifyJob(job, nil, now, threshold)).To(Equal(JobHealthFailed))
		})

		It("reports a job that never started a pod within the window as stuck", func() {
			job := newJob("stuck", withCreatedAgo(10*time.Minute))
			Expect(ClassifyJob(job, nil, now, threshold)).To(Equal(JobHealthStuck))
		})

		It("does not flag a young never-started job as stuck", func() {
			job := newJob("young", withCreatedAgo(time.Minute))
			Expect(ClassifyJob(job, nil, now, threshold)).To(Equal(JobHealthInProgress))
		})

		It("treats the threshold boundary as stuck", func() {
			job := newJob("boundary", withCreatedAgo(threshold))
			Expect(ClassifyJob(job, nil, now, threshold)).To(Equal(JobHealthStuck))
		})

		It("reports a job whose pod stayed unschedulable past the window as stuck, despite being active", func() {
			// The Job looks healthy from its own status (StartTime set, Active>0),
			// but its only Pod has been unschedulable for longer than the window.
			job := newJob("unsched", withCreatedAgo(10*time.Minute), withStartTime(10*time.Minute), withActive(1))
			pods := []corev1.Pod{unschedulablePod("unsched", 10*time.Minute, "0/3 nodes available: insufficient cpu")}
			Expect(ClassifyJob(job, pods, now, threshold)).To(Equal(JobHealthStuck))
		})

		It("does not flag a recently unschedulable pod as stuck", func() {
			job := newJob("unsched-young", withCreatedAgo(time.Minute), withStartTime(time.Minute), withActive(1))
			pods := []corev1.Pod{unschedulablePod("unsched-young", time.Minute, "transient")}
			Expect(ClassifyJob(job, pods, now, threshold)).To(Equal(JobHealthInProgress))
		})
	})

	Describe("DiagnoseJobs", func() {
		It("returns no problems for an empty list", func() {
			diagnosis := DiagnoseJobs(nil, nil, now, threshold)
			Expect(diagnosis.HasProblems()).To(BeFalse())
			Expect(diagnosis.Failed).To(BeEmpty())
			Expect(diagnosis.Stuck).To(BeEmpty())
		})

		It("returns no problems when all jobs are healthy", func() {
			jobs := []batchv1.Job{
				newJob("a", withActive(1)),
				newJob("b", withStartTime(time.Minute)),
				newJob("c"),
			}
			diagnosis := DiagnoseJobs(jobs, nil, now, threshold)
			Expect(diagnosis.HasProblems()).To(BeFalse())
		})

		It("groups failed and stuck jobs separately", func() {
			jobs := []batchv1.Job{
				newJob("healthy", withActive(1)),
				newJob("failed-1", withFailedCondition),
				newJob("stuck-1", withCreatedAgo(10*time.Minute)),
				newJob("failed-2", withFailedCondition),
			}
			diagnosis := DiagnoseJobs(jobs, nil, now, threshold)
			Expect(diagnosis.HasProblems()).To(BeTrue())
			Expect(diagnosis.Failed).To(HaveLen(2))
			Expect(diagnosis.Stuck).To(HaveLen(1))
			Expect(diagnosis.Failed[0].Name).To(Equal("failed-1"))
			Expect(diagnosis.Failed[1].Name).To(Equal("failed-2"))
			Expect(diagnosis.Stuck[0].Name).To(Equal("stuck-1"))
		})

		It("flags a job as stuck when its correlated pod is unschedulable", func() {
			jobs := []batchv1.Job{
				newJob("healthy", withActive(1)),
				newJob("sched-blocked", withCreatedAgo(10*time.Minute), withStartTime(10*time.Minute), withActive(1)),
			}
			// The unschedulable pod belongs to "sched-blocked"; the pod owned by
			// "healthy" is fine and must not cross-contaminate.
			pods := []corev1.Pod{unschedulablePod("sched-blocked", 10*time.Minute, "0/3 nodes available")}
			diagnosis := DiagnoseJobs(jobs, pods, now, threshold)
			Expect(diagnosis.Failed).To(BeEmpty())
			Expect(diagnosis.Stuck).To(HaveLen(1))
			Expect(diagnosis.Stuck[0].Name).To(Equal("sched-blocked"))
		})
	})

	Describe("UnschedulablePodReason", func() {
		It("returns empty when no pod is unschedulable", func() {
			Expect(UnschedulablePodReason(nil)).To(BeEmpty())
			Expect(UnschedulablePodReason([]corev1.Pod{{
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			}})).To(BeEmpty())
		})

		It("returns the PodScheduled message of the first unschedulable pod", func() {
			pods := []corev1.Pod{
				unschedulablePod("j", time.Minute, "0/3 nodes available: insufficient memory"),
			}
			Expect(UnschedulablePodReason(pods)).To(Equal("0/3 nodes available: insufficient memory"))
		})

		It("falls back to the condition reason when the message is empty", func() {
			pods := []corev1.Pod{unschedulablePod("j", time.Minute, "")}
			Expect(UnschedulablePodReason(pods)).To(Equal(corev1.PodReasonUnschedulable))
		})
	})

	Describe("PodsControlledByJob", func() {
		It("matches by owner reference UID and by job-name label", func() {
			job := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "j", UID: "uid-1"}}
			byUID := corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name:            "by-uid",
				OwnerReferences: []metav1.OwnerReference{{UID: "uid-1"}},
			}}
			byLabel := corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name:   "by-label",
				Labels: map[string]string{batchv1.JobNameLabel: "j"},
			}}
			unrelated := corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name:   "other",
				Labels: map[string]string{batchv1.JobNameLabel: "k"},
			}}
			got := PodsControlledByJob(&job, []corev1.Pod{byUID, byLabel, unrelated})
			Expect(got).To(HaveLen(2))
			Expect(got[0].Name).To(Equal("by-uid"))
			Expect(got[1].Name).To(Equal("by-label"))
		})
	})

	Describe("MostRecentFailedCreateEvent", func() {
		newEvent := func(reason, message string, lastSeen time.Time) corev1.Event {
			return corev1.Event{
				Reason:        reason,
				Message:       message,
				LastTimestamp: metav1.NewTime(lastSeen),
			}
		}

		It("returns a zero value when there are no events", func() {
			Expect(MostRecentFailedCreateEvent(nil).Message).To(BeEmpty())
		})

		It("ignores events whose reason is not FailedCreate", func() {
			events := []corev1.Event{
				newEvent("Scheduled", "pod scheduled", now),
				newEvent("SuccessfulCreate", "created pod", now),
			}
			Expect(MostRecentFailedCreateEvent(events).Message).To(BeEmpty())
		})

		It("returns the message of a single FailedCreate event", func() {
			events := []corev1.Event{
				newEvent(FailedCreateEventReason, "exceeded quota: pods=1", now),
			}
			result := MostRecentFailedCreateEvent(events)
			Expect(result.Message).To(Equal("exceeded quota: pods=1"))
			Expect(result.When).To(Equal(now))
		})

		It("returns the most recent FailedCreate event when several are present", func() {
			events := []corev1.Event{
				newEvent(FailedCreateEventReason, "old: webhook denied", now.Add(-10*time.Minute)),
				newEvent(FailedCreateEventReason, "new: exceeded quota", now),
				newEvent("Scheduled", "noise", now.Add(time.Hour)),
			}
			Expect(MostRecentFailedCreateEvent(events).Message).To(Equal("new: exceeded quota"))
		})

		It("falls back to FirstTimestamp then CreationTimestamp when LastTimestamp is unset", func() {
			withFirst := corev1.Event{
				Reason:         FailedCreateEventReason,
				Message:        "via first timestamp",
				FirstTimestamp: metav1.NewTime(now),
			}
			withCreation := corev1.Event{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-time.Minute))},
				Reason:     FailedCreateEventReason,
				Message:    "via creation timestamp",
			}
			result := MostRecentFailedCreateEvent([]corev1.Event{withCreation, withFirst})
			Expect(result.Message).To(Equal("via first timestamp"))
		})
	})
})
