package resource

import (
	"sync"
	"uta.edu/aces/jade-go/kernel"
)

type ResourceCache struct {
	Nodes map[string]map[ResourceType]*Resource
	ResourceSlices map[ResourceType]map[string]*ReversedResourceSliceIndex
	mutex *sync.Mutex
}

func NewResourceCache() *ResourceCache {
	return &ResourceCache{
		Nodes: make(map[string]map[ResourceType]*Resource),
		mutex: &sync.Mutex{},
	}
}

type ReversedResourceSliceIndex struct {
	ResourceID string
	NodeID string
	Type ResourceType 
	ResourceSlice *ResourceSlice
}

func (c *ResourceCache) setMaxResourceOnNode(
	resourceType ResourceType, nodeKey string, maxCap *kernel.Capacity,
) {
	if nodeKey == "" || resourceType == "" || maxCap == nil {return}
	if c.Nodes == nil {return}
	c.mutex.Lock()
	defer c.mutex.Unlock()

	isNewResource := false
	if nodeItem, ne := c.Nodes[nodeKey]; ne {
		if resource, re := nodeItem[resourceType]; re {
			resource.MaximumCapacity = maxCap.Copy()
			resource.RemainingCapacity = maxCap.Copy()
			resource.RemainingCapacity.Consume(resource.AllocatedCapacity)
		} else {
			isNewResource = true
		}
	} else {
		c.Nodes[nodeKey] = make(map[ResourceType]*Resource)
		isNewResource = true
	}
	if isNewResource {
		c.Nodes[nodeKey][resourceType] = NewResource(
			resourceType, maxCap.Copy(),
		)
	}
}

func (c *ResourceCache) mallocResourceSlice(
	resourceType ResourceType, nodeKey string,
	appId string, moduleName string, cap *kernel.Capacity,
) (bool, string) {

	success := false
	resourceId := ""
	if nodeKey == "" || resourceType == "" || cap == nil {
		return success, resourceId
	}
	if c.Nodes == nil {return success, resourceId}
	c.mutex.Lock()
	defer c.mutex.Unlock()


	if nodeItem, ne := c.Nodes[nodeKey]; ne {
		if resource, re := nodeItem[resourceType]; re {
			if resource.RemainingCapacity.GE(cap) {
				resourceSlice := NewResourceSlice(
					resourceType, appId, moduleName, cap,
				)
				if resource.Slices == nil {
					resource.Slices = make(map[string]*ResourceSlice)
				}

				if c.ResourceSlices == nil {
					c.ResourceSlices = make(map[ResourceType]map[string]*ReversedResourceSliceIndex)
				}
				if _, rse := c.ResourceSlices[resourceType]; !rse {
					c.ResourceSlices[resourceType] = make(map[string]*ReversedResourceSliceIndex)
				}
				for _, rse := c.ResourceSlices[resourceType][resourceSlice.ResourceID]; rse; {
					resourceSlice.ResourceID = genResourceID(resourceType)
				}
				resourceId = resourceSlice.ResourceID

				resource.Slices[resourceId] = resourceSlice
				resource.RemainingCapacity.Consume(cap)
				success = true

			}
		}
	}

	return success, resourceId

}

func (c *ResourceCache) freeResourceSlice(
	resourceType ResourceType, resourceId string,
) bool {
	success := false
	if resourceType == "" || resourceId == "" {
		return success
	}
	if c.Nodes == nil {return true}
	if c.ResourceSlices == nil {return true}
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if rst, rste := c.ResourceSlices[resourceType]; rste {
		if rrsi, rrse := rst[resourceId]; rrse {
			if nodeItem, ne := c.Nodes[rrsi.NodeID]; ne {
				if resource, re := nodeItem[resourceType]; re {
					if slice, se := resource.Slices[resourceId]; se {
						delete(resource.Slices, resourceId)
						resource.RemainingCapacity.Resume(slice.Capacity)
						resource.AllocatedCapacity.Consume(slice.Capacity)
						delete(rst, resourceId)
						success = true
					}
				}
			}
		}
	}

	return success
}


func (c *ResourceCache) getResourceSlice(resourceType ResourceType, resourceId string) *ResourceSlice {
	if c.Nodes == nil {return nil}
	if c.ResourceSlices == nil {return nil}

	if rst, rste := c.ResourceSlices[resourceType]; rste {
		if rrsi, rrse := rst[resourceId]; rrse {
			return rrsi.ResourceSlice
		}
	}
	return nil
}

func (c *ResourceCache) setResourceSlicePointer(
	resourceType ResourceType, resourceId string, 
	pointer interface{},
) bool {
	success := false
	if resourceId == "" {
		return success
	}
	if c.Nodes == nil {return success}
	if c.ResourceSlices == nil {return success}
	// if pointer == nil {return success}
	c.mutex.Lock()
	defer c.mutex.Unlock()

	slice := c.getResourceSlice(resourceType, resourceId)
	if slice != nil && slice.Type == resourceType {
		slice.Pointer = pointer
		success = true
	}

	return success
}

func (c *ResourceCache) getResourceSlicePointer(
	resourceType ResourceType, resourceId string,
) interface{} {
	if resourceId == "" {
		return nil
	}
	if c.Nodes == nil {return nil}
	if c.ResourceSlices == nil {return nil}
	c.mutex.Lock()
	defer c.mutex.Unlock()

	slice := c.getResourceSlice(resourceType, resourceId)
	if slice != nil && slice.Type == resourceType {
		return slice.Pointer 
	}

	return nil
}


func (c *ResourceCache) occupy(
	resourceType ResourceType, resourceId string, occupiedBy string,
	notifier *func(string),
) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	slice := c.getResourceSlice(resourceType, resourceId)
	if slice != nil {
		if slice.OccupiedBy == "" && occupiedBy != "" {
			slice.OccupiedBy = occupiedBy
			slice.Notifier = notifier
			return true
		}
	}
	return false
}

func (c *ResourceCache) release(
	resourceType ResourceType, resourceId string, occupiedBy string,
) bool {
	c.mutex.Lock()

	slice := c.getResourceSlice(resourceType, resourceId)
	if slice != nil {
		if slice.OccupiedBy != "" && slice.OccupiedBy == occupiedBy {
			slice.OccupiedBy = ""
			if slice.Notifier != nil {
				c.mutex.Unlock()
				go (*slice.Notifier)(resourceId)
				return true
			}
			c.mutex.Unlock()
			return true
		}
	}
	c.mutex.Unlock()
	return false
}

func (c *ResourceCache) isOccupied(resourceType ResourceType, resourceId string) bool {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	slice := c.getResourceSlice(resourceType, resourceId)
	if slice != nil {
		return slice.isOccupied()
	}

	return false
}

