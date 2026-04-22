package istio

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes/fake"
)

func TestIsServiceMesh2Installed(t *testing.T) {
	tests := []struct {
		name           string
		groupVersions  []string
		want           bool
	}{
		{
			name:          "SM2 installed",
			groupVersions: []string{"maistra.io/v2"},
			want:          true,
		},
		{
			name:          "SM2 not installed",
			groupVersions: []string{},
			want:          false,
		},
		{
			name:          "only SM3 installed",
			groupVersions: []string{"sailoperator.io/v1"},
			want:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := fakeDiscoveryWithGroupVersions(tt.groupVersions)
			if got := IsServiceMesh2Installed(d); got != tt.want {
				t.Errorf("IsServiceMesh2Installed() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsServiceMesh3Installed(t *testing.T) {
	tests := []struct {
		name          string
		groupVersions []string
		want          bool
	}{
		{
			name:          "SM3 installed",
			groupVersions: []string{"sailoperator.io/v1"},
			want:          true,
		},
		{
			name:          "SM3 not installed",
			groupVersions: []string{},
			want:          false,
		},
		{
			name:          "only SM2 installed",
			groupVersions: []string{"maistra.io/v2"},
			want:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := fakeDiscoveryWithGroupVersions(tt.groupVersions)
			if got := IsServiceMesh3Installed(d); got != tt.want {
				t.Errorf("IsServiceMesh3Installed() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsServiceMeshInstalled(t *testing.T) {
	tests := []struct {
		name          string
		groupVersions []string
		want          bool
	}{
		{
			name:          "SM2 installed",
			groupVersions: []string{"maistra.io/v2"},
			want:          true,
		},
		{
			name:          "SM3 installed",
			groupVersions: []string{"sailoperator.io/v1"},
			want:          true,
		},
		{
			name:          "both installed",
			groupVersions: []string{"maistra.io/v2", "sailoperator.io/v1"},
			want:          true,
		},
		{
			name:          "none installed",
			groupVersions: []string{},
			want:          false,
		},
		{
			name:          "unrelated API group",
			groupVersions: []string{"apps/v1"},
			want:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := fakeDiscoveryWithGroupVersions(tt.groupVersions)
			if got := IsServiceMeshInstalled(d); got != tt.want {
				t.Errorf("IsServiceMeshInstalled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func fakeDiscoveryWithGroupVersions(groupVersions []string) *fakediscovery.FakeDiscovery {
	client := fake.NewSimpleClientset()
	d := client.Discovery().(*fakediscovery.FakeDiscovery)

	resources := make([]*metav1.APIResourceList, 0, len(groupVersions))
	for _, gv := range groupVersions {
		resources = append(resources, &metav1.APIResourceList{GroupVersion: gv})
	}
	d.Resources = resources
	return d
}
