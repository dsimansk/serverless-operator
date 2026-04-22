package common

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/operator/pkg/apis/operator/base"
)

// Configure sets a value in the given ConfigMap under the given key.
func Configure(s *base.CommonSpec, cm, key, value string) {
	if s.Config == nil {
		s.Config = make(map[string]map[string]string, 1)
	}

	if s.Config[cm] == nil {
		s.Config[cm] = make(map[string]string, 1)
	}

	s.Config[cm][key] = value
}

// ConfigureIfUnset sets a value in the given ConfigMap under the given key if it's not
// already set.
func ConfigureIfUnset(s *base.CommonSpec, cm, key, value string) {
	if s.Config == nil {
		s.Config = make(map[string]map[string]string, 1)
	}

	if s.Config[cm] == nil {
		s.Config[cm] = make(map[string]string, 1)
	}

	if _, ok := s.Config[cm][key]; ok {
		// Already set, nothing to do here.
		return
	}
	s.Config[cm][key] = value
}

// ConfigureIfConfigmapUnset sets a value in the given ConifgMap if any configuration is not already set.
// For example, the config-domain can take an arbitrary domain as a key, so it should be used here.
func ConfigureIfConfigmapUnset(s *base.CommonSpec, cm, key, value string) {
	if s.Config == nil {
		s.Config = make(map[string]map[string]string, 1)
	}

	if s.Config[cm] == nil {
		s.Config[cm] = make(map[string]string, 1)
	}

	if len(s.Config[cm]) != 0 {
		// Already set, nothing to do here.
		return
	}

	s.Config[cm][key] = value
}

// SetAnnotationIfUnset sets an annotation on the given object if it's not already set.
func SetAnnotationIfUnset(obj metav1.Object, key, value string) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	if _, ok := annotations[key]; !ok {
		annotations[key] = value
		obj.SetAnnotations(annotations)
	}
}

// EnsureWorkloadOverride finds the workload override by name and merges the given labels
// and annotations into it without overwriting existing values. If no override exists for
// the given name, a new one is appended.
func EnsureWorkloadOverride(s *base.CommonSpec, name string, labels, annotations map[string]string) {
	for i, w := range s.Workloads {
		if w.Name == name {
			mergeIfUnset(&s.Workloads[i].Labels, labels)
			mergeIfUnset(&s.Workloads[i].Annotations, annotations)
			return
		}
	}
	s.Workloads = append(s.Workloads, base.WorkloadOverride{
		Name:        name,
		Labels:      labels,
		Annotations: annotations,
	})
}

func mergeIfUnset(dst *map[string]string, src map[string]string) {
	if len(src) == 0 {
		return
	}
	if *dst == nil {
		*dst = make(map[string]string, len(src))
	}
	for k, v := range src {
		if _, ok := (*dst)[k]; !ok {
			(*dst)[k] = v
		}
	}
}
