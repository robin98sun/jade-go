package resource

import (
	// "sync"
	"uta.edu/aces/jade-go/kernel"
)

func (c *ResourceCache) SetMaxPodCapacityOnNode(
	nodeKey string, maxCap *kernel.Capacity,
) {
	c.setMaxResourceOnNode(
		ResourceTypePod, nodeKey, maxCap,
	)
}

func (c *ResourceCache) MallocPod( 
	nodeKey string, appId string, moduleName string, cap *kernel.Capacity,
) (bool, string) {
	
	return c.mallocResourceSlice(
		ResourceTypePod, nodeKey, appId, moduleName, cap,
	)
}

func (c *ResourceCache) FreePod(resourceId string) bool {
	return c.freeResourceSlice(
		ResourceTypePod, resourceId,
	)
}

func (c *ResourceCache) SetPod(resourceId string, pod *kernel.Pod) bool {
	return c.setResourceSlicePointer(
		ResourceTypePod, resourceId, pod,
	)
}

func (c *ResourceCache) GetPod(resourceId string) *kernel.Pod {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	pointer := c.getResourceSlicePointer(ResourceTypePod, resourceId)
	if pointer != nil {
		return pointer.(*kernel.Pod)
	}
	return nil
}

func (c *ResourceCache) OccupyPod(resourceId string, occupiedBy string, releaseNotifier *func(string)) bool {
	return c.occupy(ResourceTypePod, resourceId, occupiedBy, releaseNotifier)
}

func (c *ResourceCache) ReleasePod(resourceId string, occupiedBy string) bool {
	return c.release(ResourceTypePod, resourceId, occupiedBy)
}

func (c *ResourceCache) IsPodOccupied(resourceId string) bool {
	return c.isOccupied(ResourceTypePod, resourceId)
}

func (c *ResourceCache) GetAllPods() []*kernel.Pod {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var pods []*kernel.Pod = nil
	if podCache, e := c.ResourceSlices[ResourceTypePod]; e {
		pods = []*kernel.Pod{}

		for _, rrsi := range podCache {
			pods = append(pods, rrsi.ResourceSlice.Pointer.(*kernel.Pod))
		}
	}

	return pods
}