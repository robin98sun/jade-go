package kernel

import (
	"strconv"
	// "strings"
)

type Node struct {
	Address         string `json:"address"`
	Port            int    `json:"port"`
	Protocol        string `json:"protocol"`
	Token           string `json:"token"`
	Namespace       string `json:"namespace"`
	PodName         string `json:"podName"`
	Hostname        string `json:"hostname"`
	ServiceExternal string `json:"serviceExternal"`
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
	return n.Hostname + ":" + n.Namespace + ":" + n.PodName
}

// IsAddrEmpty tells whether a node address is meaningless
func (n *Node) IsAddrEmpty() bool {
	return n.Address == "" || n.Port == 0
}

// URL is the base http/https url for the node to access
func (n *Node) URL() string {
	if n.Address != "" && n.Port != 0 {
		return n.Protocol + "://" + n.Address + ":" + strconv.Itoa(n.Port)
	}
	return ""
}
