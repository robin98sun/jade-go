package kernel

type Node struct {
	Address  string `json:"address"`
	Port     int    `json:"port"`
	Protocol string `json:"protocol"`
	Token    string `json:"token"`
}

// NewNode construct a new node instance with default values
func NewNode() *Node {
	return &Node{
		Address:  "",
		Port:     8080,
		Protocol: "http",
		Token:    "",
	}
}
