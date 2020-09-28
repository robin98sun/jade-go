package kernel

type Capacity struct {
	CPU       int64 `json:"cpu,omitempty"`
	RAM       int64 `json:"ram,omitempty"`
	Disk      int64 `json:"disk,omitempty"`
	Bandwidth int64 `json:"bandwidth,omitempty"`
}

// NewCapacity construct a new capacity instance with default values
func NewCapacity() *Capacity {
	return &Capacity{
		CPU:       100,
		RAM:       100,
		Disk:      100,
		Bandwidth: 100,
	}
}

func (c *Capacity) Copy() *Capacity {
	return &Capacity{
		CPU:       c.CPU,
		RAM:       c.RAM,
		Disk:      c.Disk,
		Bandwidth: c.Bandwidth,
	}
}

func (c *Capacity) GE(nc *Capacity) bool {
	if nc == nil {
		return true
	}
	return c.CPU >= nc.CPU && c.RAM >= nc.RAM && c.Disk >= nc.Disk && c.Bandwidth >= nc.Bandwidth
}

func (c *Capacity) Consume(nc *Capacity) {
	if nc == nil {
		return
	}
	c.CPU -= nc.CPU
	c.RAM -= nc.RAM
	c.Disk -= nc.Disk
	c.Bandwidth -= nc.Bandwidth
}

func (c *Capacity) Resume(nc *Capacity) {
	if nc == nil {
		return
	}
	c.CPU += nc.CPU
	c.RAM += nc.RAM
	c.Disk += nc.Disk
	c.Bandwidth += nc.Bandwidth
}

type CapacityStatus struct {
	MaximumCapacity   *Capacity
	RemainingCapacity *Capacity
	ReservedCapacity  *Capacity
}
