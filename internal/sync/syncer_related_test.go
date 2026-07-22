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

import (
	"testing"

	"go.uber.org/zap"

	dummyv1alpha1 "github.com/kcp-dev/api-syncagent/internal/sync/apis/dummy/v1alpha1"
	syncagentv1alpha1 "github.com/kcp-dev/api-syncagent/sdk/apis/syncagent/v1alpha1"

	"github.com/kcp-dev/logicalcluster/v3"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func TestEffectiveCleanupPolicy(t *testing.T) {
	testcases := []struct {
		name     string
		cleanup  bool
		policy   syncagentv1alpha1.RelatedResourceCleanupPolicy
		expected syncagentv1alpha1.RelatedResourceCleanupPolicy
	}{
		{
			name:     "no cleanup, no policy defaults to Orphan",
			expected: syncagentv1alpha1.RelatedResourceCleanupPolicyOrphan,
		},
		{
			name:     "legacy cleanup:true maps to OnPrimaryDeletion",
			cleanup:  true,
			expected: syncagentv1alpha1.RelatedResourceCleanupPolicyOnPrimaryDeletion,
		},
		{
			name:     "explicit Orphan wins over cleanup:false",
			policy:   syncagentv1alpha1.RelatedResourceCleanupPolicyOrphan,
			expected: syncagentv1alpha1.RelatedResourceCleanupPolicyOrphan,
		},
		{
			name:     "explicit policy wins over legacy cleanup:true",
			cleanup:  true,
			policy:   syncagentv1alpha1.RelatedResourceCleanupPolicyMatchOrigin,
			expected: syncagentv1alpha1.RelatedResourceCleanupPolicyMatchOrigin,
		},
		{
			name:     "explicit OnPrimaryDeletion without cleanup",
			policy:   syncagentv1alpha1.RelatedResourceCleanupPolicyOnPrimaryDeletion,
			expected: syncagentv1alpha1.RelatedResourceCleanupPolicyOnPrimaryDeletion,
		},
		{
			name:     "explicit MatchOrigin without cleanup",
			policy:   syncagentv1alpha1.RelatedResourceCleanupPolicyMatchOrigin,
			expected: syncagentv1alpha1.RelatedResourceCleanupPolicyMatchOrigin,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			spec := &syncagentv1alpha1.RelatedResourceSpec{
				Cleanup:       tc.cleanup,
				CleanupPolicy: tc.policy,
			}

			if got := spec.EffectiveCleanupPolicy(); got != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, got)
			}
		})
	}
}

func TestRelatedCopyLabelsSelectorRoundTrip(t *testing.T) {
	primary := &unstructured.Unstructured{}
	primary.SetName("my-primary")
	primary.SetNamespace("some-namespace")

	const (
		clusterName  = logicalcluster.Name("cluster-a")
		publishedRes = "published-resource-a"
		identifier   = "credentials"
		agentName    = "agent-1"
	)

	labelSet := relatedCopyLabels(primary, clusterName, publishedRes, identifier, agentName)

	// the labels produced for a copy must match the selector used to find them again.
	selector := relatedCopySelector(primary, clusterName, publishedRes, identifier, agentName)
	if !selector.Matches(labels.Set(labelSet)) {
		t.Errorf("selector %q does not match its own labels %v", selector, labelSet)
	}

	// a selector for a different identifier must not match.
	otherIdentifier := relatedCopySelector(primary, clusterName, publishedRes, "other", agentName)
	if otherIdentifier.Matches(labels.Set(labelSet)) {
		t.Errorf("selector for a different identifier unexpectedly matched labels %v", labelSet)
	}

	// a selector for a different agent must not match.
	otherAgent := relatedCopySelector(primary, clusterName, publishedRes, identifier, "agent-2")
	if otherAgent.Matches(labels.Set(labelSet)) {
		t.Errorf("selector for a different agent unexpectedly matched labels %v", labelSet)
	}

	// a selector for a different primary object must not match.
	otherPrimary := &unstructured.Unstructured{}
	otherPrimary.SetName("other-primary")
	otherPrimary.SetNamespace("some-namespace")
	if relatedCopySelector(otherPrimary, clusterName, publishedRes, identifier, agentName).Matches(labels.Set(labelSet)) {
		t.Errorf("selector for a different primary unexpectedly matched labels %v", labelSet)
	}

	// a selector for a primary in a different logical cluster (workspace) must not match; the
	// destination is shared across workspaces, so two primaries with identical name+namespace in
	// different clusters must not prune each other's copies.
	if relatedCopySelector(primary, "cluster-b", publishedRes, identifier, agentName).Matches(labels.Set(labelSet)) {
		t.Errorf("selector for a different cluster unexpectedly matched labels %v", labelSet)
	}

	// a selector for a different owning PublishedResource must not match; two PublishedResources that
	// project to the same Kind, reuse an identifier and have primaries sharing name+namespace must
	// not prune each other's copies.
	if relatedCopySelector(primary, clusterName, "published-resource-b", identifier, agentName).Matches(labels.Set(labelSet)) {
		t.Errorf("selector for a different published resource unexpectedly matched labels %v", labelSet)
	}

	// a cluster-scoped primary (no namespace) must omit the namespace-hash label and still
	// round-trip.
	clusterPrimary := &unstructured.Unstructured{}
	clusterPrimary.SetName("cluster-primary")
	clusterLabels := relatedCopyLabels(clusterPrimary, clusterName, publishedRes, identifier, agentName)
	if _, ok := clusterLabels[relatedPrimaryNamespaceHashLabel]; ok {
		t.Error("expected no namespace-hash label for a cluster-scoped primary")
	}
	if !relatedCopySelector(clusterPrimary, clusterName, publishedRes, identifier, agentName).Matches(labels.Set(clusterLabels)) {
		t.Errorf("cluster-scoped selector does not match its own labels %v", clusterLabels)
	}
}

func TestRememberRelatedObjectsSkipsEmptyResolution(t *testing.T) {
	const identifier = "credentials"

	relatedAnnotation := relatedObjectAnnotationPrefix + identifier + ".0"

	// A primary that already carries a related-object annotation for this identifier (from a previous
	// pass) plus an unrelated annotation that must never be touched.
	primary := newUnstructured(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-primary",
			Namespace: "default",
			Annotations: map[string]string{
				relatedAnnotation:  `{"name":"secret-copy","namespace":"default","apiVersion":"v1","kind":"Secret"}`,
				"example.com/keep": "yes",
			},
		},
	})

	client := buildFakeClient(primary)
	remote := syncSide{object: primary.DeepCopy(), client: client}

	s := &ResourceSyncer{}

	// With an empty resolved set, the informational annotations must be left untouched: no patch, no
	// requeue. Wiping them here would churn the primary object for every cleanup policy, not just the
	// pruning ones (the prune below is the authoritative cleanup for the copies themselves).
	requeue, err := s.rememberRelatedObjects(t.Context(), zap.NewNop().Sugar(), remote, identifier, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if requeue {
		t.Error("expected requeue=false for an empty resolution, but got true")
	}

	persisted := &unstructured.Unstructured{}
	persisted.SetGroupVersionKind(primary.GroupVersionKind())
	if err := client.Get(t.Context(), ctrlruntimeclient.ObjectKeyFromObject(primary), persisted); err != nil {
		t.Fatalf("failed to get persisted object: %v", err)
	}

	if got := persisted.GetAnnotations()[relatedAnnotation]; got == "" {
		t.Error("expected the pre-existing related-object annotation to be preserved, but it was wiped")
	}
	if got := persisted.GetAnnotations()["example.com/keep"]; got != "yes" {
		t.Errorf("unrelated annotation must be preserved, got %q", got)
	}
}

func TestPruneRelatedCopies(t *testing.T) {
	log := zap.NewNop().Sugar()

	const (
		clusterName  = logicalcluster.Name("cluster-a")
		publishedRes = "pr-a"
		identifier   = "credentials"
		agentName    = "agent-1"
	)

	primary := &unstructured.Unstructured{}
	primary.SetAPIVersion("v1")
	primary.SetKind("ConfigMap")
	primary.SetName("my-primary")
	primary.SetNamespace("default")

	secretGVK := schema.GroupVersionKind{Version: "v1", Kind: "Secret"}
	selector := relatedCopySelector(primary, clusterName, publishedRes, identifier, agentName)

	// makeCopy builds a Secret carrying the provenance labels for the given identifier (so it is only
	// in scope for that identifier's selector), optionally with a finalizer.
	makeCopy := func(name, namespace, forIdentifier string, withFinalizer bool) *unstructured.Unstructured {
		meta := metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    relatedCopyLabels(primary, clusterName, publishedRes, forIdentifier, agentName),
		}
		if withFinalizer {
			meta.Finalizers = []string{"example.com/test"}
		}
		return newUnstructured(&corev1.Secret{ObjectMeta: meta})
	}

	// listSecretNames returns the names of all Secrets currently present, for assertions.
	listSecretNames := func(t *testing.T, client ctrlruntimeclient.Client) sets.Set[string] {
		t.Helper()
		list := &corev1.SecretList{}
		if err := client.List(t.Context(), list); err != nil {
			t.Fatalf("failed to list secrets: %v", err)
		}
		names := sets.New[string]()
		for _, item := range list.Items {
			names.Insert(item.Name)
		}
		return names
	}

	// "foreign" is a copy for a different related resource identifier on the same primary; the
	// credentials selector must never match it, so it is never pruned.
	foreign := makeCopy("foreign-copy", "default", "other-identifier", false)

	t.Run("mid-life prune deletes copies outside the keep set and leaves the rest", func(t *testing.T) {
		keepMe := makeCopy("keep-me", "default", identifier, false)
		pruneMe := makeCopy("prune-me", "other-ns", identifier, false) // also proves cross-namespace pruning
		client := buildFakeClient(keepMe, pruneMe, foreign)

		keep := sets.New(relatedCopyKey("default", "keep-me"))
		requeue, err := (&ResourceSyncer{}).pruneRelatedCopies(t.Context(), log, syncSide{client: client}, primary, secretGVK, selector, keep, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !requeue {
			t.Error("expected requeue=true after deleting a copy")
		}

		remaining := listSecretNames(t, client)
		if !remaining.Has("keep-me") {
			t.Error("kept copy was unexpectedly deleted")
		}
		if remaining.Has("prune-me") {
			t.Error("stale copy outside the keep set was not pruned")
		}
		if !remaining.Has("foreign-copy") {
			t.Error("copy for a different identifier must not be pruned")
		}
	})

	t.Run("teardown deletes every matching copy but not foreign copies", func(t *testing.T) {
		a := makeCopy("copy-a", "default", identifier, false)
		b := makeCopy("copy-b", "default", identifier, false)
		client := buildFakeClient(a, b, foreign)

		requeue, err := (&ResourceSyncer{}).pruneRelatedCopies(t.Context(), log, syncSide{client: client}, primary, secretGVK, selector, nil, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !requeue {
			t.Error("expected requeue=true after deleting copies")
		}

		remaining := listSecretNames(t, client)
		if remaining.Has("copy-a") || remaining.Has("copy-b") {
			t.Errorf("expected all matching copies to be deleted, still present: %v", remaining)
		}
		if !remaining.Has("foreign-copy") {
			t.Error("copy for a different identifier must not be pruned")
		}
	})

	t.Run("empty match set is a no-op", func(t *testing.T) {
		client := buildFakeClient(foreign)

		requeue, err := (&ResourceSyncer{}).pruneRelatedCopies(t.Context(), log, syncSide{client: client}, primary, secretGVK, selector, nil, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if requeue {
			t.Error("expected requeue=false when nothing matches the selector")
		}
		if !listSecretNames(t, client).Has("foreign-copy") {
			t.Error("non-matching copy must be left untouched")
		}
	})

	t.Run("copy already being deleted requeues instead of erroring", func(t *testing.T) {
		deleting := makeCopy("deleting-copy", "default", identifier, true)
		client := buildFakeClient(deleting)

		// Delete it once: because it has a finalizer, the fake client only sets a deletion timestamp
		// and retains the object, mimicking a copy that is mid-deletion.
		if err := client.Delete(t.Context(), deleting); err != nil {
			t.Fatalf("failed to start deletion: %v", err)
		}

		requeue, err := (&ResourceSyncer{}).pruneRelatedCopies(t.Context(), log, syncSide{client: client}, primary, secretGVK, selector, nil, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !requeue {
			t.Error("expected requeue=true so the reconcile comes back once the copy is gone")
		}
		if !listSecretNames(t, client).Has("deleting-copy") {
			t.Error("a copy that is already being deleted must not be treated as an error or vanish early")
		}
	})
}

func TestResolveRelatedResourceObjects(t *testing.T) {
	// in kcp
	primaryObject := newUnstructured(&dummyv1alpha1.Thing{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-test-thing",
		},
		Spec: dummyv1alpha1.ThingSpec{
			Username: "original-value",
			Kink:     "taxreturns",
		},
	}, withKind("RemoteThing"))

	// on the service cluster
	primaryObjectCopy := newUnstructured(&dummyv1alpha1.Thing{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-test-thing",
		},
		Spec: dummyv1alpha1.ThingSpec{
			Username: "mutated-value",
			Kink:     "",
		},
	})

	// Create a secret that can be found by using a good reference, so we can ensure that references
	// do indeed work; all other subtests here ensure that reference support can deal with broken refs.
	dummySecret := newUnstructured(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "dummy-namespace",
			Name:      "mutated-value",
		},
	})

	kcpClient := buildFakeClient(primaryObject)
	serviceClusterClient := buildFakeClient(primaryObjectCopy, dummySecret)

	// Now we configure origin/dest as if we're syncing a Secret up from the service cluster to kcp,
	// i.e. origin=service.

	originSide := syncSide{
		client: serviceClusterClient,
		object: primaryObjectCopy,
	}

	destSide := syncSide{
		client: kcpClient,
		object: primaryObject,
		// Since this is a just a regular kube client, we do not need to set clusterName/clusterPath.
	}

	testcases := []struct {
		name            string
		objectSpec      syncagentv1alpha1.RelatedResourceObject
		expectedSecrets int
	}{
		{
			name: "valid reference to an existing object",
			objectSpec: syncagentv1alpha1.RelatedResourceObject{
				RelatedResourceObjectSpec: syncagentv1alpha1.RelatedResourceObjectSpec{
					Reference: &syncagentv1alpha1.RelatedResourceObjectReference{
						Path: "spec.username",
					},
				},
				Namespace: &syncagentv1alpha1.RelatedResourceObjectSpec{
					Template: &syncagentv1alpha1.TemplateExpression{
						Template: "dummy-namespace",
					},
				},
			},
			expectedSecrets: 1,
		},
		{
			name: "valid template to an existing object",
			objectSpec: syncagentv1alpha1.RelatedResourceObject{
				RelatedResourceObjectSpec: syncagentv1alpha1.RelatedResourceObjectSpec{
					Template: &syncagentv1alpha1.TemplateExpression{
						Template: "{{ .Object.spec.username }}",
					},
				},
				Namespace: &syncagentv1alpha1.RelatedResourceObjectSpec{
					Template: &syncagentv1alpha1.TemplateExpression{
						Template: "dummy-namespace",
					},
				},
			},
			expectedSecrets: 1,
		},
		{
			name: "valid reference but target object doesn't exist [yet?]",
			objectSpec: syncagentv1alpha1.RelatedResourceObject{
				RelatedResourceObjectSpec: syncagentv1alpha1.RelatedResourceObjectSpec{
					Reference: &syncagentv1alpha1.RelatedResourceObjectReference{
						Path: "spec.username",
					},
				},
				Namespace: &syncagentv1alpha1.RelatedResourceObjectSpec{
					Template: &syncagentv1alpha1.TemplateExpression{
						Template: "nonexisting-namespace",
					},
				},
			},
			expectedSecrets: 0,
		},
		{
			name: "valid template but target object doesn't exist [yet?]",
			objectSpec: syncagentv1alpha1.RelatedResourceObject{
				RelatedResourceObjectSpec: syncagentv1alpha1.RelatedResourceObjectSpec{
					Template: &syncagentv1alpha1.TemplateExpression{
						Template: "{{ .Object.spec.username }}",
					},
				},
				Namespace: &syncagentv1alpha1.RelatedResourceObjectSpec{
					Template: &syncagentv1alpha1.TemplateExpression{
						Template: "nonexisting-namespace",
					},
				},
			},
			expectedSecrets: 0,
		},
		{
			name: "valid reference to an empty field",
			objectSpec: syncagentv1alpha1.RelatedResourceObject{
				RelatedResourceObjectSpec: syncagentv1alpha1.RelatedResourceObjectSpec{
					Reference: &syncagentv1alpha1.RelatedResourceObjectReference{
						Path: "spec.kink",
					},
				},
				Namespace: &syncagentv1alpha1.RelatedResourceObjectSpec{
					Template: &syncagentv1alpha1.TemplateExpression{
						Template: "dummy-namespace",
					},
				},
			},
			expectedSecrets: 0,
		},
		{
			name: "valid template to an empty field",
			objectSpec: syncagentv1alpha1.RelatedResourceObject{
				RelatedResourceObjectSpec: syncagentv1alpha1.RelatedResourceObjectSpec{
					Template: &syncagentv1alpha1.TemplateExpression{
						Template: "{{ .Object.spec.kink }}",
					},
				},
				Namespace: &syncagentv1alpha1.RelatedResourceObjectSpec{
					Template: &syncagentv1alpha1.TemplateExpression{
						Template: "dummy-namespace",
					},
				},
			},
			expectedSecrets: 0,
		},
		{
			name: "referring to an omitempty field",
			objectSpec: syncagentv1alpha1.RelatedResourceObject{
				RelatedResourceObjectSpec: syncagentv1alpha1.RelatedResourceObjectSpec{
					Reference: &syncagentv1alpha1.RelatedResourceObjectReference{
						Path: "spec.address",
					},
				},
				Namespace: &syncagentv1alpha1.RelatedResourceObjectSpec{
					Template: &syncagentv1alpha1.TemplateExpression{
						Template: "dummy-namespace",
					},
				},
			},
			expectedSecrets: 0,
		},
		{
			name: "templating an omitempty field",
			objectSpec: syncagentv1alpha1.RelatedResourceObject{
				RelatedResourceObjectSpec: syncagentv1alpha1.RelatedResourceObjectSpec{
					Template: &syncagentv1alpha1.TemplateExpression{
						Template: "{{ .Object.spec.address }}",
					},
				},
				Namespace: &syncagentv1alpha1.RelatedResourceObjectSpec{
					Template: &syncagentv1alpha1.TemplateExpression{
						Template: "dummy-namespace",
					},
				},
			},
			expectedSecrets: 0,
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			pubRes := syncagentv1alpha1.RelatedResourceSpec{
				Identifier: "test",
				Origin:     syncagentv1alpha1.RelatedResourceOriginService,
				Kind:       "Secret",
				Object:     testcase.objectSpec,
			}

			foundObjects, err := resolveRelatedResourceObjects(t.Context(), originSide, destSide, pubRes)
			if err != nil {
				t.Fatalf("Failed to resolve related objects: %v", err)
			}
			if len(foundObjects) != testcase.expectedSecrets {
				t.Fatalf("Expected %d related object (Secret) to be found, but found %d.", testcase.expectedSecrets, len(foundObjects))
			}
		})
	}
}
