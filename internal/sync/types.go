/*
Copyright 2025 The KCP Authors.

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

package sync

import ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"

const (
	// deletionFinalizer is the finalizer put on remote objects to prevent
	// them from being deleted before the local objects can be cleaned up.
	deletionFinalizer = "syncagent.kcp.io/cleanup"

	// The following 4 labels/annotations are put on local objects to link them to their
	// origin remote objects. Note that the cluster *path* label is optional and
	// has to be enabled per PublishedResource.

	remoteObjectClusterLabel       = "syncagent.kcp.io/remote-object-cluster"
	remoteObjectNamespaceHashLabel = "syncagent.kcp.io/remote-object-namespace-hash"
	remoteObjectNameHashLabel      = "syncagent.kcp.io/remote-object-name-hash"

	remoteObjectNamespaceAnnotation = "syncagent.kcp.io/remote-object-namespace"
	remoteObjectNameAnnotation      = "syncagent.kcp.io/remote-object-name"

	remoteObjectWorkspacePathAnnotation = "syncagent.kcp.io/remote-object-workspace-path"

	// agentNameLabel contains the Sync Agent's name and is used to allow multiple Sync Agents
	// on the same service cluster, syncing *the same* API to different kcp's.
	agentNameLabel = "syncagent.kcp.io/agent-name"

	// objectStateLabelName is put on object state Secrets to allow for easier mass deletions
	// if ever necessary.
	objectStateLabelName = "syncagent.kcp.io/object-state"

	// objectStateLabelValue is the value of the objectStateLabelName label.
	objectStateLabelValue = "true"

	// relatedObjectAnnotationPrefix is the prefix for the annotation that is placed on
	// objects in the kcp workspaces, informing the user about the existence of a related
	// object. The identifier of the related object is appended to this to form the
	// full annotation name, the annotation value is a JSON string containing GVK and
	// metadata of the related object.
	relatedObjectAnnotationPrefix = "related-resources.syncagent.kcp.io/"

	// The following labels/annotations are put on the destination copies of related resources
	// to link them back to their owning primary object and the related resource identifier.
	// They allow the agent to List all copies belonging to a specific primary + identifier so
	// that it can prune copies whose origin object no longer exists (cleanupPolicy: MatchOrigin)
	// or delete all copies on primary teardown. Together they must uniquely identify the owning
	// primary, otherwise one primary's prune selector could match (and delete) another's copies:
	// the destination is shared across all kcp workspaces (so the primary's logical cluster is
	// included) and across all PublishedResources (so the owning PublishedResource is included).
	// Names/namespaces and the PublishedResource name can exceed the 63-character label limit or
	// contain invalid characters, so they are hashed; the plaintext values are kept as annotations
	// for humans.

	relatedPrimaryClusterLabel        = "syncagent.kcp.io/related-primary-cluster"
	relatedPublishedResourceHashLabel = "syncagent.kcp.io/related-published-resource-hash"
	relatedPrimaryNamespaceHashLabel  = "syncagent.kcp.io/related-primary-namespace-hash"
	relatedPrimaryNameHashLabel       = "syncagent.kcp.io/related-primary-name-hash"
	relatedIdentifierLabel            = "syncagent.kcp.io/related-identifier"

	relatedPrimaryNamespaceAnnotation = "syncagent.kcp.io/related-primary-namespace"
	relatedPrimaryNameAnnotation      = "syncagent.kcp.io/related-primary-name"
)

func OwnedBy(obj ctrlruntimeclient.Object, agentName string) bool {
	return obj.GetLabels()[agentNameLabel] == agentName
}
