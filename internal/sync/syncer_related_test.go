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

	dummyv1alpha1 "github.com/kcp-dev/api-syncagent/internal/sync/apis/dummy/v1alpha1"
	syncagentv1alpha1 "github.com/kcp-dev/api-syncagent/sdk/apis/syncagent/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
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
		identifier = "credentials"
		agentName  = "agent-1"
	)

	labelSet := relatedCopyLabels(primary, identifier, agentName)

	// the labels produced for a copy must match the selector used to find them again.
	selector := relatedCopySelector(primary, identifier, agentName)
	if !selector.Matches(labels.Set(labelSet)) {
		t.Errorf("selector %q does not match its own labels %v", selector, labelSet)
	}

	// a selector for a different identifier must not match.
	otherIdentifier := relatedCopySelector(primary, "other", agentName)
	if otherIdentifier.Matches(labels.Set(labelSet)) {
		t.Errorf("selector for a different identifier unexpectedly matched labels %v", labelSet)
	}

	// a selector for a different agent must not match.
	otherAgent := relatedCopySelector(primary, identifier, "agent-2")
	if otherAgent.Matches(labels.Set(labelSet)) {
		t.Errorf("selector for a different agent unexpectedly matched labels %v", labelSet)
	}

	// a selector for a different primary object must not match.
	otherPrimary := &unstructured.Unstructured{}
	otherPrimary.SetName("other-primary")
	otherPrimary.SetNamespace("some-namespace")
	if relatedCopySelector(otherPrimary, identifier, agentName).Matches(labels.Set(labelSet)) {
		t.Errorf("selector for a different primary unexpectedly matched labels %v", labelSet)
	}

	// a cluster-scoped primary (no namespace) must omit the namespace-hash label and still
	// round-trip.
	clusterPrimary := &unstructured.Unstructured{}
	clusterPrimary.SetName("cluster-primary")
	clusterLabels := relatedCopyLabels(clusterPrimary, identifier, agentName)
	if _, ok := clusterLabels[relatedPrimaryNamespaceHashLabel]; ok {
		t.Error("expected no namespace-hash label for a cluster-scoped primary")
	}
	if !relatedCopySelector(clusterPrimary, identifier, agentName).Matches(labels.Set(clusterLabels)) {
		t.Errorf("cluster-scoped selector does not match its own labels %v", clusterLabels)
	}
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
