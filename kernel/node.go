package kernel

import (
	"strconv"
)

type Node struct {
	Address         string `json:"address,omitempty"`
	Port            int    `json:"port,omitempty"`
	Protocol        string `json:"protocol,omitempty"`
	Token           string `json:"token,omitempty"`
	Namespace       string `json:"namespace,omitempty"`
	PodName         string `json:"podName,omitempty"`
	Hostname        string `json:"hostname,omitempty"`
	ServiceExternal string `json:"serviceExternal,omitempty"`
	// Roles           []string `json:"roles,omitempty"` // master(the top most), linker(only one subnode), aggregator(has subnodes), worker(no subnode)
}

// NewNode construct a new node instance with default values
func NewNode() *Node {
	return &Node{
		Address:   "",
		Port:      0,
		Protocol:  "http",
		Token:     "",
		Namespace: "jade-app",
		PodName:   "",
		Hostname:  "",
	}
}

// Key is used to store node in cache
func (n *Node) Key() string {
	if !n.IsAddrEmpty() {
		return n.URL()
	} else if n.Hostname != "" && n.Namespace != "" && n.PodName != "" {
		return n.Hostname + ":" + n.Namespace + ":" + n.PodName
	}
	return ""
}

// IsAddrEmpty tells whether a node address is meaningless
func (n *Node) IsAddrEmpty() bool {
	return n.Address == "" || n.Port == 0 || n.Protocol == ""
}

// URL is the base http/https url for the node to access
func (n *Node) URL() string {
	if n.Address != "" && n.Port != 0 {
		return n.Protocol + "://" + n.Address + ":" + strconv.Itoa(n.Port)
	}
	return ""
}

// MiniNode is to get a copy of minimum content to transfer on the network
func (n *Node) MiniNode() *Node {
	return &Node{
		Address:  n.Address,
		Port:     n.Port,
		Protocol: n.Protocol,
		Token:    n.Token,
	}
}
