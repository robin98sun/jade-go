package resource_manager

import(
	"sync"
	"math"
)


type CPUResourceType string
const(
	CPUResourceTypeShares CPUResourceType = "cpu-shares"
	CPUResourceTypePeriod CPUResourceType = "cpu-period"
	CPUResourceTypeQuota  CPUResourceType = "cpu-quota"
)


type CPUResourceCache struct {
	
	mutex 			*sync.Mutex

	CPUCores        int
	TotalShares 	int

	Pods  map[string]*CPUResourceItem

	DefaultShares   int
	DefaultPeriod   int

}

type CPUResourceItem struct {
	NR_Period int `json:"nr_period,omitempty"`
	NR_Throttled int `json:"nr_throttled,omitempty"`
	Period int `json:"period,omitempty"`
	Quota int `json:"quota,omitempty"`
	Shares int `json:"shares,omitempty"`
	Throttled_Time int `json:"throttled_time,omitempty"`
	Type string `json:"type,omitempty"`
	UID string `json:"uid,omitempty"`
}

func (i *CPUResourceItem) GetNormalizedCPUCores() float64 {
	if i.Period == 0 {
		return 0
	}
	return float64(i.Quota)/float64(i.Period)
}

func NewCPUResourceCache() *CPUResourceCache {
	return &CPUResourceCache{
		mutex: &sync.Mutex{},
		CPUCores: 0,
		TotalShares: 0,
		Pods: make(map[string]*CPUResourceItem),
		DefaultShares: 2,
		DefaultPeriod: 100000,
	}
}

func (c *CPUResourceCache) SetCPUCores(cores int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.CPUCores = cores
}

func (c *CPUResourceCache) SetTotalShares(shares int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.TotalShares = shares
}

func (c *CPUResourceCache) NewCPUResourceItem() *CPUResourceItem {
	return &CPUResourceItem{
		Shares: c.DefaultShares,
		Period: c.DefaultPeriod,
	}
}

func (c *CPUResourceCache) GetCPUResourceItem(podKey string) *CPUResourceItem {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if item, e := c.Pods[podKey]; e{
		return item
	}
	return nil
}

func (c *CPUResourceCache) SetPodResource(resourceType CPUResourceType, podKey string, value int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, e := c.Pods[podKey]; !e{
		c.Pods[podKey] = c.NewCPUResourceItem()
	}

	if resourceType == CPUResourceTypeShares {
		c.Pods[podKey].Shares = value
	} else if resourceType == CPUResourceTypePeriod {
		c.Pods[podKey].Period = value
	} else if resourceType == CPUResourceTypeQuota {
		c.Pods[podKey].Quota = value
	}
}

func (c *CPUResourceCache) SetPodResources(podKey string, shares int, period int, quota int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, e := c.Pods[podKey]; !e{
		c.Pods[podKey] = c.NewCPUResourceItem()
	}
	c.Pods[podKey].Shares = shares
	c.Pods[podKey].Period = period
	c.Pods[podKey].Quota  = quota
}

func (c *CPUResourceCache) SetPodQuotaByDefaults(podKey string, quota int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if _, e := c.Pods[podKey]; !e{
		c.Pods[podKey] = c.NewCPUResourceItem()
	}
	c.Pods[podKey].Quota = quota
}

func (c *CPUResourceCache) GetPodResource(resourceType CPUResourceType, podKey string) int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if item, e := c.Pods[podKey]; e {
		if resourceType == CPUResourceTypeShares {
			return item.Shares
		} else if resourceType == CPUResourceTypePeriod {
			return item.Period
		} else if resourceType == CPUResourceTypeQuota {
			return item.Quota
		}
	}

	return 0
}

func (c *CPUResourceCache) GetRemainingShares() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	used := 0
	for _, item := range c.Pods {
		used += item.Shares
	}
	return c.TotalShares - used
}

func (c *CPUResourceCache) GetNormalizedCPUCores(podKey string, exclusive bool) float64 {
	if exclusive {
		c.mutex.Lock()
	}

	result := float64(0)
	if item, e := c.Pods[podKey]; e {
		totalInUseShares := 0
		for _, tmpItem := range c.Pods {
			if tmpItem.Type == "besteffort" { 
				continue 
			}else if tmpItem.Type == "fixed" {
				totalInUseShares += tmpItem.Shares
			}
		}
		coreShares := float64(0)
		if totalInUseShares > 0 {
			coreShares = float64(c.CPUCores) * float64(item.Shares) / float64(totalInUseShares)	
		} 
		coreQuota := coreShares
		if item.Period > 0 && item.Quota > 0 {
			coreQuota = float64(item.Quota) / float64(item.Period)
		}
		if coreShares == 0 {
			result = coreQuota
		} else {
			result = math.Min(coreShares, coreQuota)
		}	
	}

	if exclusive {
		c.mutex.Unlock()
	}
	return result
}

func (c *CPUResourceCache) GetRemainingCPUCores() float64 {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	normalizedQuota := float64(0)

	for podKey, item := range c.Pods {
		if item.Type == "fixed" {
			nc := c.GetNormalizedCPUCores(podKey, false)
			normalizedQuota += nc
		}
	}

	return (float64(c.CPUCores) - normalizedQuota)

}

func (c *CPUResourceCache) UpdatePods(pods map[string]*CPUResourceItem) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.Pods = pods
}

func (c *CPUResourceCache) CalcQuotaForTargetCPUCores(podKey string, targetCores float64) (int, int, float64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	targetQuota := -1
	deltaQuota := 0
	maxCores := float64(0)
	if item, e := c.Pods[podKey]; e{
		totalInUseShares := 0
		for _, tmpItem := range c.Pods {
			if tmpItem.Type == "besteffort" { 
				continue 
			}else if tmpItem.Type == "fixed" {
				totalInUseShares += tmpItem.Shares
			}
		}
		coreShares := float64(0)
		if totalInUseShares > 0 {
			coreShares = float64(c.CPUCores) * float64(item.Shares) / float64(totalInUseShares)	
		} 
		maxCores = coreShares

		if targetCores > coreShares {
			maximumQuota := int(math.Round(float64(c.CPUCores)*float64(item.Period)))
			targetQuota = maximumQuota
			deltaQuota = maximumQuota - item.Quota
		} else if c.CPUCores > 0 {
			targetQuota = int(math.Round(float64(targetCores) / float64(c.CPUCores) * float64(item.Period)))
			deltaQuota = targetQuota - item.Quota
		}
	}

	return targetQuota, deltaQuota, maxCores
}



















