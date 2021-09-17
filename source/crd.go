/*
Copyright 2018,2021 The Kubernetes Authors.

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
	"encoding/json"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"sigs.k8s.io/external-dns/endpoint"
)

// crdSource is an implementation of Source that provides endpoints by listing
// specified CRD and fetching Endpoints embedded in Spec.
type crdSource struct {
	client           dynamic.Interface
	namespace        string
	crdResource      schema.GroupVersionResource
	annotationFilter string
	labelFilter      string
}

// NewCRDSource creates a new crdSource with the given config.
func NewCRDSource(client dynamic.Interface, namespace, version, resource string, annotationFilter string, labelFilter string) (Source, error) {
	// Parse the version into its parts
	groupVersion, err := schema.ParseGroupVersion(version)
	if err != nil {
		return nil, err
	}

	source := &crdSource{
		crdResource:      schema.GroupVersionResource{Group: groupVersion.Group, Version: groupVersion.Version, Resource: resource},
		namespace:        namespace,
		annotationFilter: annotationFilter,
		labelFilter:      labelFilter,
		client:           client,
	}
	return source, nil
}

func (cs *crdSource) AddEventHandler(ctx context.Context, handler func()) {
}

// Endpoints returns endpoint objects.
func (cs *crdSource) Endpoints(ctx context.Context) ([]*endpoint.Endpoint, error) {
	endpoints := []*endpoint.Endpoint{}

	var (
		result []endpoint.DNSEndpoint
		err    error
	)

	if cs.labelFilter != "" {
		result, err = cs.listEndpoints(ctx, &metav1.ListOptions{LabelSelector: cs.labelFilter})
	} else {
		result, err = cs.listEndpoints(ctx, &metav1.ListOptions{})
	}
	if err != nil {
		return nil, err
	}

	result, err = filterByAnnotations(result, cs.annotationFilter)
	if err != nil {
		return nil, err
	}

	for _, dnsEndpoint := range result {
		// Make sure that all endpoints have targets for A or CNAME type
		crdEndpoints := []*endpoint.Endpoint{}
		for _, ep := range dnsEndpoint.Spec.Endpoints {
			if (ep.RecordType == "CNAME" || ep.RecordType == "A" || ep.RecordType == "AAAA") && len(ep.Targets) < 1 {
				log.Warnf("Endpoint %s with DNSName %s has an empty list of targets", dnsEndpoint.ObjectMeta.Name, ep.DNSName)
				continue
			}

			illegalTarget := false
			for _, target := range ep.Targets {
				if strings.HasSuffix(target, ".") {
					illegalTarget = true
					break
				}
			}
			if illegalTarget {
				log.Warnf("Endpoint %s with DNSName %s has an illegal target. The subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com')", dnsEndpoint.ObjectMeta.Name, ep.DNSName)
				continue
			}

			if ep.Labels == nil {
				ep.Labels = endpoint.NewLabels()
			}

			crdEndpoints = append(crdEndpoints, ep)
		}

		setResourceLabel(&dnsEndpoint, crdEndpoints)
		endpoints = append(endpoints, crdEndpoints...)

		err = cs.updateObservedGeneration(ctx, &dnsEndpoint)
		if err != nil {
			log.Warnf("Could not update ObservedGeneration of the CRD: %v", err)
		}
	}

	return endpoints, nil
}

// listEndpoints queries the server for the user-specified resources
// and converts them into DNSEndpoint objects.
func (cs *crdSource) listEndpoints(ctx context.Context, opts *metav1.ListOptions) ([]endpoint.DNSEndpoint, error) {
	rawEndpoints, err := cs.client.
		Resource(cs.crdResource).
		Namespace(cs.namespace).
		List(ctx, *opts)
	if err != nil {
		return nil, err
	}
	log.Debugf("%d %v found", len(rawEndpoints.Items), cs.crdResource)

	// Convert each Unstructured into a DNSEndpoint
	converter := runtime.DefaultUnstructuredConverter
	endpoints := make([]endpoint.DNSEndpoint, len(rawEndpoints.Items))
	for i, rawEndpoint := range rawEndpoints.Items {
		endpoints[i] = endpoint.DNSEndpoint{}
		if err = converter.FromUnstructured(rawEndpoint.UnstructuredContent(), &endpoints[i]); err != nil {
			return nil, err
		}
	}

	return endpoints, err
}

// updateObservedGeneration updates the observedGeneration field in
// the object status to indicate the most recent version/generation
// that we've already examined. We use the "Patch" verb to minimize
// conflicts.
func (cs *crdSource) updateObservedGeneration(ctx context.Context, dnsEndpoint *endpoint.DNSEndpoint) error {
	// Return if we don't need to do anything
	if dnsEndpoint.Status.ObservedGeneration == dnsEndpoint.Generation {
		return nil
	}

	// Update the ObservedGeneration to match the current Generation
	payload, err := json.Marshal(endpoint.DNSEndpoint{
		Status: endpoint.DNSEndpointStatus{
			ObservedGeneration: dnsEndpoint.Generation,
		},
	})
	if err != nil {
		return err
	}
	_, err = cs.client.
		Resource(cs.crdResource).
		Namespace(dnsEndpoint.Namespace).
		Patch(ctx, dnsEndpoint.Name, types.MergePatchType, payload, metav1.PatchOptions{}, "status")
	return err
}

// filterByAnnotations filters a list of dnsendpoints by a given annotation selector.
func filterByAnnotations(dnsendpoints []endpoint.DNSEndpoint, annotationFilter string) ([]endpoint.DNSEndpoint, error) {
	labelSelector, err := metav1.ParseToLabelSelector(annotationFilter)
	if err != nil {
		return nil, err
	}
	selector, err := metav1.LabelSelectorAsSelector(labelSelector)
	if err != nil {
		return nil, err
	}

	// empty filter returns original list
	if selector.Empty() {
		return dnsendpoints, nil
	}

	filteredList := []endpoint.DNSEndpoint{}

	for _, dnsendpoint := range dnsendpoints {
		// convert the dnsendpoint' annotations to an equivalent label selector
		annotations := labels.Set(dnsendpoint.Annotations)

		// include dnsendpoint if its annotations match the selector
		if selector.Matches(annotations) {
			filteredList = append(filteredList, dnsendpoint)
		}
	}

	return filteredList, nil
}

func setResourceLabel(crd *endpoint.DNSEndpoint, endpoints []*endpoint.Endpoint) {
	for _, ep := range endpoints {
		ep.Labels[endpoint.ResourceLabelKey] = fmt.Sprintf("crd/%s/%s", crd.ObjectMeta.Namespace, crd.ObjectMeta.Name)
	}
}
