package kernel

type Capacity struct {
	CPU       int `json:"cpu"`
	RAM       int `json:"ram"`
	Disk      int `json:"disk"`
	Bandwidth int `json:"bandwidth"`
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
