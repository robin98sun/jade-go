package kernel

type Capability struct {
	Name string `json:"name"`
	API  string `json:"api"`
}

// NewCapability construct a capability instance with default values
func NewCapability() *Capability {
	return &Capability{
		Name: "",
		API:  "",
	}
}
