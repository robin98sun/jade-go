package conf

import (
	"aces/jade-go/kernel"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

// Conf configuration data structure in memory
type Conf struct {
	UpperNode    *kernel.Node        `json:"upperNode"`
	SelfNode     *kernel.Node        `json:"selfNode"`
	Capabilities []kernel.Capability `json:"capabilities"`
	Capacity     *kernel.Capacity    `json:"capacity"`
}

// NewConfiguration construct a new configuration instance with default values
func NewConfiguration() *Conf {
	c := &Conf{}
	c.UpperNode = kernel.NewNode()
	c.SelfNode = kernel.NewNode()
	c.Capabilities = []kernel.Capability{}
	c.Capacity = kernel.NewCapacity()
	return c
}

// ReadConfFromEnv Read configuration from environment variables
func ReadConfFromEnv() *Conf {
	c := NewConfiguration()
	for _, e := range os.Environ() {
		pair := strings.SplitN(e, "=", 2)
		envName := pair[0]
		envValue := pair[1]

		nameParts := strings.SplitN(envName, "_", -1)
		if len(nameParts) < 3 || nameParts[0] != "JADE" {
			continue
		}

		// for UpperNode and SelfNode
		switch nameParts[2] {
		case "ADDRESS":
			if nameParts[1] == "UPPERNODE" {
				c.UpperNode.Address = envValue
			} else if nameParts[1] == "SELFNODE" {
				c.SelfNode.Address = envValue
			}
		case "PORT":
			port, err := strconv.Atoi(envValue)
			if err == nil {
				if nameParts[1] == "UPPERNODE" {
					c.UpperNode.Port = port
				} else if nameParts[1] == "SELFNODE" {
					c.SelfNode.Port = port
				}
			}
		case "PROTOCOL":
			if nameParts[1] == "UPPERNODE" {
				c.UpperNode.Protocol = envValue
			} else if nameParts[1] == "SELFNODE" {
				c.SelfNode.Protocol = envValue
			}
		case "TOKEN":
			if nameParts[1] == "UPPERNODE" {
				c.UpperNode.Token = envValue
			} else if nameParts[1] == "SELFNODE" {
				c.SelfNode.Token = envValue
			}
		case "HOSTNAME":
			if nameParts[1] == "UPPERNODE" {
				c.UpperNode.Hostname = envValue
			} else if nameParts[1] == "SELFNODE" {
				c.SelfNode.Hostname = envValue
			}
		case "PODNAME":
			if nameParts[1] == "UPPERNODE" {
				c.UpperNode.PodName = envValue
			} else if nameParts[1] == "SELFNODE" {
				c.SelfNode.PodName = envValue
			}
		case "NAMESPACE":
			if nameParts[1] == "UPPERNODE" {
				c.UpperNode.Namespace = envValue
			} else if nameParts[1] == "SELFNODE" {
				c.SelfNode.Namespace = envValue
			}
		case "SERVICEEXTERNAL":
			if nameParts[1] == "UPPERNODE" {
				c.UpperNode.ServiceExternal = envValue
			} else if nameParts[1] == "SELFNODE" {
				c.SelfNode.ServiceExternal = envValue
			}
		}

		// for capacity
		if nameParts[1] == "CAPACITY" {
			v, err := strconv.Atoi(envValue)
			if err == nil {
				switch nameParts[2] {
				case "CPU":
					c.Capacity.CPU = v
				case "RAM":
					c.Capacity.RAM = v
				case "DISK":
					c.Capacity.Disk = v
				case "BANDWIDTH":
					c.Capacity.Bandwidth = v
				}
			}
		} else if nameParts[1] == "CAPABILITY" && len(nameParts) == 4 {
			i, err := strconv.Atoi(nameParts[2])
			if err == nil {
				if i >= len(c.Capabilities) {
					for x := len(c.Capabilities); x <= i; x++ {
						capability := *kernel.NewCapability()
						c.Capabilities = append(c.Capabilities, capability)
					}
				}
				switch nameParts[3] {
				case "NAME":
					c.Capabilities[i].Name = envValue
				case "API":
					c.Capabilities[i].API = envValue
				}
			}
		}

	}
	return c
}

// ReadConfFromJSON Read configuration from JSON string or json file
func ReadConfFromJSON(jsonstr string, isFile bool) *Conf {
	c := NewConfiguration()
	if isFile {
		file, _ := ioutil.ReadFile(jsonstr)
		_ = json.Unmarshal([]byte(file), c)
	} else {
		_ = json.Unmarshal([]byte(jsonstr), c)
	}
	return c
}

// PrintJSONasEnv convert JSON file to env variables and print on stdout
func PrintJSONasEnv(jsonfile string) {
	file, _ := ioutil.ReadFile(jsonfile)
	c := NewConfiguration()
	_ = json.Unmarshal([]byte(file), c)
	// Print self node
	fmt.Printf("JADE_SELFNODE_ADDRESS=%v\n", c.SelfNode.Address)
	fmt.Printf("JADE_SELFNODE_PORT=%v\n", c.SelfNode.Port)
	fmt.Printf("JADE_SELFNODE_PROTOCOL=%v\n", c.SelfNode.Protocol)
	fmt.Printf("JADE_SELFNODE_TOKEN=%v\n", c.SelfNode.Token)
	fmt.Printf("JADE_SELFNODE_HOSTNAME=%v\n", c.SelfNode.Hostname)
	fmt.Printf("JADE_SELFNODE_NAMESPACE=%v\n", c.SelfNode.Namespace)
	fmt.Printf("JADE_SELFNODE_PODNAME=%v\n", c.SelfNode.PodName)
	fmt.Printf("JADE_SELFNODE_SERVICEEXTERNAL=%v\n", c.SelfNode.ServiceExternal)
	// Print upper node
	fmt.Printf("JADE_UPPERNODE_ADDRESS=%s\n", c.UpperNode.Address)
	fmt.Printf("JADE_UPPERNODE_PORT=%v\n", c.UpperNode.Port)
	fmt.Printf("JADE_UPPERNODE_PROTOCOL=%v\n", c.UpperNode.Protocol)
	fmt.Printf("JADE_UPPERNODE_TOKEN=%v\n", c.UpperNode.Token)
	fmt.Printf("JADE_UPPERNODE_HOSTNAME=%v\n", c.UpperNode.Hostname)
	fmt.Printf("JADE_UPPERNODE_NAMESPACE=%v\n", c.UpperNode.Namespace)
	fmt.Printf("JADE_UPPERNODE_PODNAME=%v\n", c.UpperNode.PodName)
	fmt.Printf("JADE_UPPERNODE_SERVICEEXTERNAL=%v\n", c.UpperNode.ServiceExternal)
	// Print capacity
	fmt.Printf("JADE_CAPACITY_CPU=%v\n", c.Capacity.CPU)
	fmt.Printf("JADE_CAPACITY_RAM=%v\n", c.Capacity.RAM)
	fmt.Printf("JADE_CAPACITY_DISK=%v\n", c.Capacity.Disk)
	fmt.Printf("JADE_CAPACITY_BANDWIDTH=%v\n", c.Capacity.Bandwidth)
	// Print capabilities
	for i, v := range c.Capabilities {
		fmt.Printf("JADE_CAPABILITY_%v_NAME=%v\n", i, v.Name)
		fmt.Printf("JADE_CAPABILITY_%v_API=%v\n", i, v.API)
	}
}
