package istio

import (
	"k8s.io/client-go/discovery"
)

const (
	// MaistraAPIGroup is the API group for Service Mesh 2 (Maistra).
	MaistraAPIGroup = "maistra.io"
	// SailAPIGroup is the API group for Service Mesh 3 (Sail operator).
	SailAPIGroup = "sailoperator.io"
)

// IsServiceMesh2Installed checks if Service Mesh 2 (Maistra) is installed
// by looking for the maistra.io API group.
func IsServiceMesh2Installed(d discovery.DiscoveryInterface) bool {
	return hasAPIGroup(d, MaistraAPIGroup)
}

// IsServiceMesh3Installed checks if Service Mesh 3 (Sail operator) is installed
// by looking for the sailoperator.io API group.
func IsServiceMesh3Installed(d discovery.DiscoveryInterface) bool {
	return hasAPIGroup(d, SailAPIGroup)
}

// IsServiceMeshInstalled checks if any version of Service Mesh (2 or 3) is installed.
func IsServiceMeshInstalled(d discovery.DiscoveryInterface) bool {
	return IsServiceMesh2Installed(d) || IsServiceMesh3Installed(d)
}

func hasAPIGroup(d discovery.DiscoveryInterface, groupName string) bool {
	groups, err := d.ServerGroups()
	if err != nil {
		return false
	}
	for _, g := range groups.Groups {
		if g.Name == groupName {
			return true
		}
	}
	return false
}
