package resource

import (
	"uta.edu/aces/jade-go/kernel"
)


type ResourceType string
const (
	ResourceTypePod ResourceType = "pod"
)

// Resource and Resource Slice
type Resource struct {
	Type ResourceType
	MaximumCapacity *kernel.Capacity
	RemainingCapacity *kernel.Capacity
	AllocatedCapacity *kernel.Capacity
	Slices map[string]*ResourceSlice
}

func NewResource(resourceType ResourceType, maxCap *kernel.Capacity) *Resource {
	return &Resource{
		Type: resourceType,
		MaximumCapacity: maxCap,
		RemainingCapacity: maxCap,
		AllocatedCapacity: kernel.NewCapacity(),
		Slices: make(map[string]*ResourceSlice),
	}
}

type ResourceSlice struct {
	Type ResourceType
	OccupiedBy string
	ApplicationID string
	ModuleName  string
	Capacity *kernel.Capacity
	ResourceID string
	Tag string
	Pointer interface{}
	Notifier *func(string)
}

func genResourceID(resourceType ResourceType) string {
	return string(resourceType)+"-"+kernel.RandomString()
}

func NewResourceSlice(
	resourceType ResourceType, 
	appId string, moduleName string, cap *kernel.Capacity,
) *ResourceSlice {
	return &ResourceSlice{
		Type: resourceType,
		OccupiedBy: "",
		ApplicationID: appId,
		ModuleName: moduleName,
		Capacity: cap,
		ResourceID: genResourceID(resourceType),
		Tag: "",
	}
}

func (s *ResourceSlice) isOccupied() bool {
	if s == nil {return false}

	if s.OccupiedBy != "" {return true}

	return false
}
// END of Resource and Resource Slice