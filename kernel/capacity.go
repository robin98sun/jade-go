package kernel

type Capacity struct {
	CPU       int `json:"cpu,omitempty"`
	RAM       int `json:"ram,omitempty"`
	Disk      int `json:"disk,omitempty"`
	Bandwidth int `json:"bandwidth,omitempty"`
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

func (c *Capacity) GE(cap *Capacity) bool {
	return c.CPU >= cap.CPU && c.RAM >= cap.RAM && c.Disk >= cap.Disk && c.Bandwidth >= cap.Bandwidth
}

func (c *Capacity) Consume(cap *Capacity) {
	c.CPU -= cap.CPU
	c.RAM -= cap.RAM
	c.Disk -= cap.Disk
	c.Bandwidth -= cap.Bandwidth
}

func (c *Capacity) Resume(cap *Capacity) {
	c.CPU += cap.CPU
	c.RAM += cap.RAM
	c.Disk += cap.Disk
	c.Bandwidth += cap.Bandwidth
}
