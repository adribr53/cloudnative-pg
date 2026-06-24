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
)

// DefaultJobStuckThreshold is how long a Job may exist without starting a Pod
// before it is considered stuck (a structural problem the operator cannot fix:
// quota, admission, scheduling).
const DefaultJobStuckThreshold = 5 * time.Minute

// JobHealth classifies the state of a single Job from the operator's viewpoint.
type JobHealth string

const (
	// JobHealthInProgress: running, or failing within its back-off limit.
	JobHealthInProgress JobHealth = "InProgress"

	// JobHealthFailed: JobFailed condition is True (e.g. back-off exceeded).
	JobHealthFailed JobHealth = "Failed"

	// JobHealthStuck: no Pod started within the threshold; not marked failed.
	JobHealthStuck JobHealth = "Stuck"
)

// JobDiagnosis groups offending Jobs by the problem detected.
type JobDiagnosis struct {
	// Failed holds the Jobs whose JobFailed condition is True.
	Failed []batchv1.Job

	// Stuck holds the Jobs that never started a Pod within the threshold.
	Stuck []batchv1.Job
}

// HasProblems returns true when at least one Job is failed or stuck.
func (d JobDiagnosis) HasProblems() bool {
	return len(d.Failed) > 0 || len(d.Stuck) > 0
}

// ClassifyJob returns the health of a single Job. jobPods must be the Pods owned
// by job (see PodsControlledByJob); pass nil when unavailable.
func ClassifyJob(job batchv1.Job, jobPods []corev1.Pod, now time.Time, stuckThreshold time.Duration) JobHealth {
	if JobHasFailed(job) {
		return JobHealthFailed
	}

	// Checked before the Active/StartTime short-circuit below: an unschedulable
	// Pod sets both yet never makes progress, so it must win.
	for i := range jobPods {
		if IsPodUnschedulable(&jobPods[i]) &&
			!jobPods[i].CreationTimestamp.IsZero() &&
			now.Sub(jobPods[i].CreationTimestamp.Time) >= stuckThreshold {
			return JobHealthStuck
		}
	}

	if job.Status.StartTime != nil || job.Status.Active > 0 {
		return JobHealthInProgress
	}

	// No Pod ever started: anchor on CreationTimestamp since StartTime is what's
	// missing (quota, admission, LimitRange).
	if !job.CreationTimestamp.IsZero() &&
		now.Sub(job.CreationTimestamp.Time) >= stuckThreshold {
		return JobHealthStuck
	}

	return JobHealthInProgress
}

// DiagnoseJobs classifies the Jobs and returns the failed or stuck ones. pods may
// include Pods unrelated to the Jobs; each Job is correlated by owner reference.
// Pure function: now is passed in so it stays deterministic and testable.
func DiagnoseJobs(jobs []batchv1.Job, pods []corev1.Pod, now time.Time, stuckThreshold time.Duration) JobDiagnosis {
	var diagnosis JobDiagnosis
	for i := range jobs {
		jobPods := PodsControlledByJob(&jobs[i], pods)
		switch ClassifyJob(jobs[i], jobPods, now, stuckThreshold) {
		case JobHealthFailed:
			diagnosis.Failed = append(diagnosis.Failed, jobs[i])
		case JobHealthStuck:
			diagnosis.Stuck = append(diagnosis.Stuck, jobs[i])
		case JobHealthInProgress:
		}
	}
	return diagnosis
}

// PodsControlledByJob returns the pods belonging to job, matched by owner UID and
// falling back to the Job name label when the UID is not populated.
func PodsControlledByJob(job *batchv1.Job, pods []corev1.Pod) []corev1.Pod {
	var result []corev1.Pod
	for i := range pods {
		if podBelongsToJob(&pods[i], job) {
			result = append(result, pods[i])
		}
	}
	return result
}

func podBelongsToJob(pod *corev1.Pod, job *batchv1.Job) bool {
	for _, ref := range pod.OwnerReferences {
		if job.UID != "" && ref.UID == job.UID {
			return true
		}
	}
	return pod.Labels[batchv1.JobNameLabel] == job.Name && job.Name != ""
}

// UnschedulablePodReason returns the PodScheduled message (or reason) of the
// first unschedulable Pod, or "". Preferred over the FailedScheduling event
// since the condition persists while the Pod stays Pending.
func UnschedulablePodReason(pods []corev1.Pod) string {
	for i := range pods {
		if !IsPodUnschedulable(&pods[i]) {
			continue
		}
		for _, c := range pods[i].Status.Conditions {
			if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse {
				if c.Message != "" {
					return c.Message
				}
				return c.Reason
			}
		}
	}
	return ""
}

// FailedCreateEventReason is the reason Kubernetes sets on a Job Event when the
// controller cannot create the Pod at all (quota, admission webhook, LimitRange).
const FailedCreateEventReason = "FailedCreate"

// FailedCreateEvent is a FailedCreate event's message and timestamp. A zero value
// (empty Message) means none was found.
type FailedCreateEvent struct {
	Message string
	When    time.Time
}

// MostRecentFailedCreateEvent returns the most recent FailedCreate event, or a
// zero value when none is present.
func MostRecentFailedCreateEvent(events []corev1.Event) FailedCreateEvent {
	var result FailedCreateEvent
	for i := range events {
		event := &events[i]
		if event.Reason != FailedCreateEventReason {
			continue
		}
		when := eventTime(event)
		// >= so the last of several same-timestamp events wins deterministically.
		if result.Message == "" || !when.Before(result.When) {
			result = FailedCreateEvent{Message: event.Message, When: when}
		}
	}
	return result
}

// eventTime returns the event's last occurrence, falling back to first then
// creation time.
func eventTime(event *corev1.Event) time.Time {
	if !event.LastTimestamp.IsZero() {
		return event.LastTimestamp.Time
	}
	if !event.FirstTimestamp.IsZero() {
		return event.FirstTimestamp.Time
	}
	return event.CreationTimestamp.Time
}
