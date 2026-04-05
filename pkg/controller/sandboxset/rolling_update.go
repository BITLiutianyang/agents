/*
Copyright 2025.

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

package sandboxset

import (
	"context"
	"sort"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/klog/v2"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/sandbox-manager/consts"
	"github.com/openkruise/agents/pkg/utils/sandboxutils"
)

// UpdateGroupedSandboxes extends GroupedSandboxes with update-specific groupings.
type UpdateGroupedSandboxes struct {
	// UpdatedCreating are unclaimed sandboxes being created with the update revision.
	UpdatedCreating []*agentsv1alpha1.Sandbox

	// UpdatedAvailable are unclaimed available sandboxes with the update revision.
	UpdatedAvailable []*agentsv1alpha1.Sandbox

	// OldCreating are unclaimed sandboxes being created with an old revision.
	OldCreating []*agentsv1alpha1.Sandbox

	// OldAvailable are unclaimed available sandboxes with an old revision.
	OldAvailable []*agentsv1alpha1.Sandbox
}

// UpdateInfo holds all the computed values for a rolling update.
type UpdateInfo struct {
	// CurrentUpdated is the current number of unclaimed pods with update revision.
	CurrentUpdated int

	// ToUpdate is the number of pods that still need to be updated.
	ToUpdate int

	// AllowedSurge is the remaining surge budget for this round.
	AllowedSurge int

	// AllowedUnavailable is the remaining unavailable budget for this round.
	AllowedUnavailable int
}

// buildUpdateGroups builds update-specific groupings from existing GroupedSandboxes.
// It categorizes unclaimed Creating and Available sandboxes by whether their template hash
// matches the update revision. Claimed sandboxes are excluded from update consideration.
func buildUpdateGroups(groups GroupedSandboxes, updateRevision string) *UpdateGroupedSandboxes {
	updateGroups := &UpdateGroupedSandboxes{}

	// Categorize Creating sandboxes by revision (skip claimed)
	for _, sbx := range groups.Creating {
		if isSandboxClaimed(sbx) {
			continue
		}
		revision := sbx.Labels[agentsv1alpha1.LabelTemplateHash]
		if revision == updateRevision {
			updateGroups.UpdatedCreating = append(updateGroups.UpdatedCreating, sbx)
		} else {
			updateGroups.OldCreating = append(updateGroups.OldCreating, sbx)
		}
	}

	// Categorize Available sandboxes by revision (skip claimed)
	for _, sbx := range groups.Available {
		if isSandboxClaimed(sbx) {
			continue
		}
		revision := sbx.Labels[agentsv1alpha1.LabelTemplateHash]
		if revision == updateRevision {
			updateGroups.UpdatedAvailable = append(updateGroups.UpdatedAvailable, sbx)
		} else {
			updateGroups.OldAvailable = append(updateGroups.OldAvailable, sbx)
		}
	}

	return updateGroups
}

// isSandboxClaimed checks if a sandbox has been claimed.
func isSandboxClaimed(sbx *agentsv1alpha1.Sandbox) bool {
	// Check if the sandbox is marked as claimed
	if sbx.Labels[agentsv1alpha1.LabelSandboxIsClaimed] == agentsv1alpha1.True {
		return true
	}
	// Check if the owner reference is not SandboxSet (claimed sandboxes have ownerRef removed)
	if !sandboxutils.IsControlledBySandboxSet(sbx) {
		return true
	}
	return false
}

// needsUpdate checks if any sandboxes need to be updated.
func needsUpdate(updateGroups *UpdateGroupedSandboxes) bool {
	oldPodsCount := len(updateGroups.OldCreating) + len(updateGroups.OldAvailable)
	return oldPodsCount > 0
}

// isUpdateComplete checks if the update is complete.
func isUpdateComplete(info *UpdateInfo) bool {
	return info.ToUpdate == 0
}

// calculateUpdateInfo calculates the update info based on the current state and update strategy.
func calculateUpdateInfo(sbs *agentsv1alpha1.SandboxSet, updateGroups *UpdateGroupedSandboxes) *UpdateInfo {
	info := &UpdateInfo{}

	// Calculate current updated counts
	info.CurrentUpdated = len(updateGroups.UpdatedCreating) + len(updateGroups.UpdatedAvailable)

	// Calculate how many still need to be updated
	info.ToUpdate = int(sbs.Spec.Replicas) - info.CurrentUpdated

	// AllowedSurge is the remaining surge budget after accounting for UpdatedCreating.
	// OldCreating pods are deleted freely (no budget needed)
	info.AllowedSurge = max(getMaxSurgePods(sbs, int(sbs.Spec.Replicas))-len(updateGroups.UpdatedCreating), 0)
	// AllowedUnavailable is the remaining unavailable budget after accounting for UpdatedCreating.
	// UpdatedCreating pods are not yet available, so they already consume the unavailable budget.
	info.AllowedUnavailable = max(getMaxUnavailablePods(sbs, int(sbs.Spec.Replicas))-len(updateGroups.UpdatedCreating), 0)

	return info
}

// getMaxSurgePods calculates the max surge pods allowed.
func getMaxSurgePods(sbs *agentsv1alpha1.SandboxSet, replicas int) int {
	// Default to 20%
	maxSurge := intstr.FromString("20%")
	if sbs.Spec.UpdateStrategy.MaxSurge != nil {
		maxSurge = *sbs.Spec.UpdateStrategy.MaxSurge
	}
	value, err := intstr.GetScaledValueFromIntOrPercent(intstr.ValueOrDefault(&maxSurge, intstr.FromInt(0)), replicas, true)
	if err != nil {
		value = (replicas * 20 / 100)
		if replicas*20%100 > 0 {
			value++
		}
	}
	return value
}

// getMaxUnavailablePods calculates the max unavailable pods allowed.
func getMaxUnavailablePods(sbs *agentsv1alpha1.SandboxSet, replicas int) int {
	// Default to 20%
	maxUnavailable := intstr.FromString("20%")
	if sbs.Spec.UpdateStrategy.MaxUnavailable != nil {
		maxUnavailable = *sbs.Spec.UpdateStrategy.MaxUnavailable
	}
	value, err := intstr.GetScaledValueFromIntOrPercent(intstr.ValueOrDefault(&maxUnavailable, intstr.FromInt(0)), replicas, false)
	if err != nil {
		value = replicas * 20 / 100
	}
	return value
}

// logUpdateInfo logs the update info for debugging.
func logUpdateInfo(ctx context.Context, info *UpdateInfo) {
	log := logf.FromContext(ctx)
	log.Info("update info calculated",
		"currentUpdated", info.CurrentUpdated,
		"toUpdate", info.ToUpdate,
		"allowedSurge", info.AllowedSurge,
		"allowedUnavailable", info.AllowedUnavailable)
}

// performRollingUpdate performs the rolling update logic.
// Returns the number of sandboxes created and deleted.
func (r *Reconciler) performRollingUpdate(
	ctx context.Context,
	sbs *agentsv1alpha1.SandboxSet,
	updateGroups *UpdateGroupedSandboxes,
	updateInfo *UpdateInfo,
	updateRevision string,
) (int, int, error) {
	log := logf.FromContext(ctx)
	logUpdateInfo(ctx, updateInfo)

	var totalCreated, totalDeleted int

	// Phase 1: Delete all OldCreating sandboxes freely (they are not available, no budget needed)
	oldCreating := make([]*agentsv1alpha1.Sandbox, len(updateGroups.OldCreating))
	copy(oldCreating, updateGroups.OldCreating)
	for _, sbx := range oldCreating {
		if err := r.deleteSandboxForUpdate(ctx, sbs, sbx); err != nil {
			log.Error(err, "failed to delete old creating sandbox", "sandbox", klog.KObj(sbx))
			return totalCreated, totalDeleted, err
		}
		totalDeleted++
	}

	// Phase 2: Handle OldAvailable sandboxes with surge/unavailable budget
	remainingToUpdate := updateInfo.ToUpdate - len(updateGroups.OldCreating)
	if remainingToUpdate <= 0 {
		log.Info("rolling update plan", "toUpdate", updateInfo.ToUpdate, "deletedOldCreating", totalDeleted,
			"created", 0, "deletedOldAvailable", 0)
		return totalCreated, totalDeleted, nil
	}

	// Sort OldAvailable by oldest first
	oldAvailable := make([]*agentsv1alpha1.Sandbox, len(updateGroups.OldAvailable))
	copy(oldAvailable, updateGroups.OldAvailable)
	sort.Slice(oldAvailable, func(i, j int) bool {
		return oldAvailable[i].CreationTimestamp.Before(&oldAvailable[j].CreationTimestamp)
	})

	var createCount, deleteCount int

	// Try to create new pods first if we have surge budget
	if updateInfo.AllowedSurge > 0 && remainingToUpdate > 0 {
		createCount = min(remainingToUpdate, updateInfo.AllowedSurge, len(oldAvailable))
	}

	// If we can't create (or still need more), delete old available pods to make room
	if createCount == 0 && remainingToUpdate > 0 {
		deleteCount = min(remainingToUpdate, updateInfo.AllowedUnavailable, len(oldAvailable))
	}

	log.Info("rolling update plan", "toUpdate", updateInfo.ToUpdate, "deletedOldCreating", totalDeleted,
		"createCount", createCount, "deleteOldAvailableCount", deleteCount)

	// Create new sandboxes with update revision
	for i := 0; i < createCount; i++ {
		_, err := r.createSandbox(ctx, sbs, updateRevision)
		if err != nil {
			log.Error(err, "failed to create sandbox during rolling update")
			return totalCreated, totalDeleted, err
		}
		totalCreated++
	}

	// Delete old available sandboxes
	for i := 0; i < deleteCount && i < len(oldAvailable); i++ {
		sbx := oldAvailable[i]
		if err := r.deleteSandboxForUpdate(ctx, sbs, sbx); err != nil {
			log.Error(err, "failed to delete sandbox during rolling update", "sandbox", klog.KObj(sbx))
			return totalCreated, totalDeleted, err
		}
		totalDeleted++
	}

	return totalCreated, totalDeleted, nil
}

// deleteSandboxForUpdate deletes a sandbox as part of rolling update.
func (r *Reconciler) deleteSandboxForUpdate(ctx context.Context, sbs *agentsv1alpha1.SandboxSet, sbx *agentsv1alpha1.Sandbox) error {
	log := logf.FromContext(ctx).WithValues("sandbox", klog.KObj(sbx))
	log.V(consts.DebugLogLevel).Info("deleting sandbox for rolling update")

	// Check if already being deleted
	if sbx.DeletionTimestamp != nil {
		return nil
	}

	// Delete the sandbox
	if err := r.Delete(ctx, sbx); err != nil {
		// ignore NotFound, because it may have been deleted by scaleDown in the same reconcile
		if errors.IsNotFound(err) {
			log.V(consts.DebugLogLevel).Info("sandbox already deleted, skipping")
			return nil
		}
		return err
	}

	r.Recorder.Eventf(sbs, "Normal", "RollingUpdate", "Deleted sandbox %s for update", klog.KObj(sbx))
	return nil
}
