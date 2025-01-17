/*
Copyright 2018 The Kubernetes Authors.

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

package source

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicFake "k8s.io/client-go/dynamic/fake"

	"sigs.k8s.io/external-dns/endpoint"
)

type CRDSuite struct {
	suite.Suite
}

func fakeRESTClient(endpoints []*endpoint.Endpoint, apiVersion, kind, namespace, name string, annotations map[string]string, labels map[string]string, t *testing.T) dynamic.Interface {
	scheme := runtime.NewScheme()
	groupVersion, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return nil
	}
	metav1.AddToGroupVersion(scheme, groupVersion)
	scheme.AddKnownTypes(groupVersion,
		&endpoint.DNSEndpoint{},
		&endpoint.DNSEndpointList{},
	)

	dnsEndpoint := &endpoint.DNSEndpoint{
		TypeMeta: metav1.TypeMeta{
			APIVersion: apiVersion,
			Kind:       kind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: annotations,
			Labels:      labels,
			Generation:  1,
		},
		Spec: endpoint.DNSEndpointSpec{
			Endpoints: endpoints,
		},
	}

	client := dynamicFake.NewSimpleDynamicClient(scheme, dnsEndpoint)
	return client
}

// FIXME: re-enable
// func TestCRDSource(t *testing.T) {
// 	suite.Run(t, new(CRDSuite))
// 	t.Run("Interface", testCRDSourceImplementsSource)
// 	t.Run("Endpoints", testCRDSourceEndpoints)
// }

// testCRDSourceImplementsSource tests that crdSource is a valid Source.
func testCRDSourceImplementsSource(t *testing.T) {
	require.Implements(t, (*Source)(nil), new(crdSource))
}

// testCRDSourceEndpoints tests various scenarios of using CRD source.
func testCRDSourceEndpoints(t *testing.T) {
	for _, ti := range []struct {
		title                string
		registeredNamespace  string
		namespace            string
		registeredAPIVersion string
		apiVersion           string
		registeredKind       string
		kind                 string
		endpoints            []*endpoint.Endpoint
		expectEndpoints      bool
		expectError          bool
		annotationFilter     string
		labelFilter          string
		annotations          map[string]string
		labels               map[string]string
	}{
		{
			title:                "invalid crd api version",
			registeredAPIVersion: "test.k8s.io/v1alpha1",
			apiVersion:           "blah.k8s.io/v1alpha1",
			registeredKind:       "DNSEndpoint",
			kind:                 "DNSEndpoint",
			endpoints: []*endpoint.Endpoint{
				{DNSName: "abc.example.org",
					Targets:    endpoint.Targets{"1.2.3.4"},
					RecordType: endpoint.RecordTypeA,
					RecordTTL:  180,
				},
			},
			expectEndpoints: false,
			expectError:     true,
		},
		{
			title:                "invalid crd kind",
			registeredAPIVersion: "test.k8s.io/v1alpha1",
			apiVersion:           "test.k8s.io/v1alpha1",
			registeredKind:       "DNSEndpoint",
			kind:                 "JustEndpoint",
			endpoints: []*endpoint.Endpoint{
				{DNSName: "abc.example.org",
					Targets:    endpoint.Targets{"1.2.3.4"},
					RecordType: endpoint.RecordTypeA,
					RecordTTL:  180,
				},
			},
			expectEndpoints: false,
			expectError:     true,
		},
		{
			title:                "endpoints within a specific namespace",
			registeredAPIVersion: "test.k8s.io/v1alpha1",
			apiVersion:           "test.k8s.io/v1alpha1",
			registeredKind:       "DNSEndpoint",
			kind:                 "DNSEndpoint",
			namespace:            "foo",
			registeredNamespace:  "foo",
			endpoints: []*endpoint.Endpoint{
				{DNSName: "abc.example.org",
					Targets:    endpoint.Targets{"1.2.3.4"},
					RecordType: endpoint.RecordTypeA,
					RecordTTL:  180,
				},
			},
			expectEndpoints: true,
			expectError:     false,
		},
		{
			title:                "no endpoints within a specific namespace",
			registeredAPIVersion: "test.k8s.io/v1alpha1",
			apiVersion:           "test.k8s.io/v1alpha1",
			registeredKind:       "DNSEndpoint",
			kind:                 "DNSEndpoint",
			namespace:            "foo",
			registeredNamespace:  "bar",
			endpoints: []*endpoint.Endpoint{
				{DNSName: "abc.example.org",
					Targets:    endpoint.Targets{"1.2.3.4"},
					RecordType: endpoint.RecordTypeA,
					RecordTTL:  180,
				},
			},
			expectEndpoints: false,
			expectError:     false,
		},
		{
			title:                "invalid crd with no targets",
			registeredAPIVersion: "test.k8s.io/v1alpha1",
			apiVersion:           "test.k8s.io/v1alpha1",
			registeredKind:       "DNSEndpoint",
			kind:                 "DNSEndpoint",
			namespace:            "foo",
			registeredNamespace:  "foo",
			endpoints: []*endpoint.Endpoint{
				{DNSName: "abc.example.org",
					Targets:    endpoint.Targets{},
					RecordType: endpoint.RecordTypeA,
					RecordTTL:  180,
				},
			},
			expectEndpoints: false,
			expectError:     false,
		},
		{
			title:                "valid crd gvk with single endpoint",
			registeredAPIVersion: "test.k8s.io/v1alpha1",
			apiVersion:           "test.k8s.io/v1alpha1",
			registeredKind:       "DNSEndpoint",
			kind:                 "DNSEndpoint",
			namespace:            "foo",
			registeredNamespace:  "foo",
			endpoints: []*endpoint.Endpoint{
				{DNSName: "abc.example.org",
					Targets:    endpoint.Targets{"1.2.3.4"},
					RecordType: endpoint.RecordTypeA,
					RecordTTL:  180,
				},
			},
			expectEndpoints: true,
			expectError:     false,
		},
		{
			title:                "valid crd gvk with multiple endpoints",
			registeredAPIVersion: "test.k8s.io/v1alpha1",
			apiVersion:           "test.k8s.io/v1alpha1",
			registeredKind:       "DNSEndpoint",
			kind:                 "DNSEndpoint",
			namespace:            "foo",
			registeredNamespace:  "foo",
			endpoints: []*endpoint.Endpoint{
				{DNSName: "abc.example.org",
					Targets:    endpoint.Targets{"1.2.3.4"},
					RecordType: endpoint.RecordTypeA,
					RecordTTL:  180,
				},
				{DNSName: "xyz.example.org",
					Targets:    endpoint.Targets{"abc.example.org"},
					RecordType: endpoint.RecordTypeCNAME,
					RecordTTL:  180,
				},
			},
			expectEndpoints: true,
			expectError:     false,
		},
		{
			title:                "valid crd gvk with annotation and non matching annotation filter",
			registeredAPIVersion: "test.k8s.io/v1alpha1",
			apiVersion:           "test.k8s.io/v1alpha1",
			registeredKind:       "DNSEndpoint",
			kind:                 "DNSEndpoint",
			namespace:            "foo",
			registeredNamespace:  "foo",
			annotations:          map[string]string{"test": "that"},
			annotationFilter:     "test=filter_something_else",
			endpoints: []*endpoint.Endpoint{
				{DNSName: "abc.example.org",
					Targets:    endpoint.Targets{"1.2.3.4"},
					RecordType: endpoint.RecordTypeA,
					RecordTTL:  180,
				},
			},
			expectEndpoints: false,
			expectError:     false,
		},
		{
			title:                "valid crd gvk with annotation and matching annotation filter",
			registeredAPIVersion: "test.k8s.io/v1alpha1",
			apiVersion:           "test.k8s.io/v1alpha1",
			registeredKind:       "DNSEndpoint",
			kind:                 "DNSEndpoint",
			namespace:            "foo",
			registeredNamespace:  "foo",
			annotations:          map[string]string{"test": "that"},
			annotationFilter:     "test=that",
			endpoints: []*endpoint.Endpoint{
				{DNSName: "abc.example.org",
					Targets:    endpoint.Targets{"1.2.3.4"},
					RecordType: endpoint.RecordTypeA,
					RecordTTL:  180,
				},
			},
			expectEndpoints: true,
			expectError:     false,
		},
		{
			title:                "valid crd gvk with label and non matching label filter",
			registeredAPIVersion: "test.k8s.io/v1alpha1",
			apiVersion:           "test.k8s.io/v1alpha1",
			registeredKind:       "DNSEndpoint",
			kind:                 "DNSEndpoint",
			namespace:            "foo",
			registeredNamespace:  "foo",
			labels:               map[string]string{"test": "that"},
			labelFilter:          "test=filter_something_else",
			endpoints: []*endpoint.Endpoint{
				{DNSName: "abc.example.org",
					Targets:    endpoint.Targets{"1.2.3.4"},
					RecordType: endpoint.RecordTypeA,
					RecordTTL:  180,
				},
			},
			expectEndpoints: false,
			expectError:     false,
		},
		{
			title:                "valid crd gvk with label and matching label filter",
			registeredAPIVersion: "test.k8s.io/v1alpha1",
			apiVersion:           "test.k8s.io/v1alpha1",
			registeredKind:       "DNSEndpoint",
			kind:                 "DNSEndpoint",
			namespace:            "foo",
			registeredNamespace:  "foo",
			labels:               map[string]string{"test": "that"},
			labelFilter:          "test=that",
			endpoints: []*endpoint.Endpoint{
				{DNSName: "abc.example.org",
					Targets:    endpoint.Targets{"1.2.3.4"},
					RecordType: endpoint.RecordTypeA,
					RecordTTL:  180,
				},
			},
			expectEndpoints: true,
			expectError:     false,
		},
		{
			title:                "Create NS record",
			registeredAPIVersion: "test.k8s.io/v1alpha1",
			apiVersion:           "test.k8s.io/v1alpha1",
			registeredKind:       "DNSEndpoint",
			kind:                 "DNSEndpoint",
			namespace:            "foo",
			registeredNamespace:  "foo",
			labels:               map[string]string{"test": "that"},
			labelFilter:          "test=that",
			endpoints: []*endpoint.Endpoint{
				{DNSName: "abc.example.org",
					Targets:    endpoint.Targets{"ns1.k8s.io", "ns2.k8s.io"},
					RecordType: endpoint.RecordTypeNS,
					RecordTTL:  180,
				},
			},
			expectEndpoints: true,
			expectError:     false,
		},
	} {
		ti := ti
		t.Run(ti.title, func(t *testing.T) {
			// t.Parallel()

			restClient := fakeRESTClient(ti.endpoints, ti.registeredAPIVersion, ti.registeredKind, ti.registeredNamespace, "test", ti.annotations, ti.labels, t)
			labelSelector, err := labels.Parse(ti.labelFilter)
			require.NoError(t, err)

			cs, err := NewCRDSource(restClient, ti.namespace, ti.apiVersion, ti.kind, ti.annotationFilter, labelSelector)
			require.NoError(t, err)

			receivedEndpoints, err := cs.Endpoints(context.Background())
			if ti.expectError {
				require.Errorf(t, err, "Received err %v", err)
			} else {
				require.NoErrorf(t, err, "Received err %v", err)
			}

			if len(receivedEndpoints) == 0 && !ti.expectEndpoints {
				return
			}

			if err == nil {
				validateCRDResource(t, cs, ti.expectError)
			}

			// Validate received endpoints against expected endpoints.
			validateEndpoints(t, receivedEndpoints, ti.endpoints)
		})
	}
}

func validateCRDResource(t *testing.T, src Source, expectError bool) {
	cs := src.(*crdSource)
	result, err := cs.List(context.Background(), &metav1.ListOptions{})
	if expectError {
		require.Errorf(t, err, "Received err %v", err)
	} else {
		require.NoErrorf(t, err, "Received err %v", err)
	}

	for _, dnsEndpoint := range result {
		if dnsEndpoint.Status.ObservedGeneration != dnsEndpoint.Generation {
			require.Errorf(t, err, "Unexpected CRD resource result: ObservedGenerations <%v> is not equal to Generation<%v>", dnsEndpoint.Status.ObservedGeneration, dnsEndpoint.Generation)
		}
	}
}
