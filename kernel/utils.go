package kernel

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

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

		// for capacity and capabilities
		if nameParts[1] == "CAPACITY" {
			v, err := strconv.ParseInt(envValue, 10, 64)
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
						capability := *NewCapability()
						c.Capabilities = append(c.Capabilities, &capability)
					}
				}
				switch nameParts[3] {
				case "NAME":
					c.Capabilities[i].Name = envValue
				case "API":
					c.Capabilities[i].API = envValue
				}
				c.Capabilities[i].ParseAPI()
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
	if len(c.Capabilities) > 0 {
		for _, cap := range c.Capabilities {
			cap.ParseAPI()
		}
	}
	return c
}

func ConfToString(c *Conf) string {
	content := ""
	if c.SelfNode != nil {
		content += fmt.Sprintf("JADE_SELFNODE_ADDRESS=%v\n", c.SelfNode.Address)
		content += fmt.Sprintf("JADE_SELFNODE_PORT=%v\n", c.SelfNode.Port)
		content += fmt.Sprintf("JADE_SELFNODE_PROTOCOL=%v\n", c.SelfNode.Protocol)
		content += fmt.Sprintf("JADE_SELFNODE_TOKEN=%v\n", c.SelfNode.Token)
		content += fmt.Sprintf("JADE_SELFNODE_HOSTNAME=%v\n", c.SelfNode.Hostname)
		content += fmt.Sprintf("JADE_SELFNODE_NAMESPACE=%v\n", c.SelfNode.Namespace)
		content += fmt.Sprintf("JADE_SELFNODE_PODNAME=%v\n", c.SelfNode.PodName)
		content += fmt.Sprintf("JADE_SELFNODE_SERVICEEXTERNAL=%v\n", c.SelfNode.ServiceExternal)
	}
	if c.UpperNode != nil {
		// Print upper node
		content += fmt.Sprintf("JADE_UPPERNODE_ADDRESS=%s\n", c.UpperNode.Address)
		content += fmt.Sprintf("JADE_UPPERNODE_PORT=%v\n", c.UpperNode.Port)
		content += fmt.Sprintf("JADE_UPPERNODE_PROTOCOL=%v\n", c.UpperNode.Protocol)
		content += fmt.Sprintf("JADE_UPPERNODE_TOKEN=%v\n", c.UpperNode.Token)
		content += fmt.Sprintf("JADE_UPPERNODE_HOSTNAME=%v\n", c.UpperNode.Hostname)
		content += fmt.Sprintf("JADE_UPPERNODE_NAMESPACE=%v\n", c.UpperNode.Namespace)
		content += fmt.Sprintf("JADE_UPPERNODE_PODNAME=%v\n", c.UpperNode.PodName)
		content += fmt.Sprintf("JADE_UPPERNODE_SERVICEEXTERNAL=%v\n", c.UpperNode.ServiceExternal)
	}
	if c.Capacity != nil {
		// Print capacity
		content += fmt.Sprintf("JADE_CAPACITY_CPU=%v\n", c.Capacity.CPU)
		content += fmt.Sprintf("JADE_CAPACITY_RAM=%v\n", c.Capacity.RAM)
		content += fmt.Sprintf("JADE_CAPACITY_DISK=%v\n", c.Capacity.Disk)
		content += fmt.Sprintf("JADE_CAPACITY_BANDWIDTH=%v\n", c.Capacity.Bandwidth)
	}
	if c.Capabilities != nil {
		// Print capabilities
		for i, v := range c.Capabilities {
			content += fmt.Sprintf("JADE_CAPABILITY_%v_NAME=%v\n", i, v.Name)
			content += fmt.Sprintf("JADE_CAPABILITY_%v_API=%v\n", i, v.API)
		}
	}
	return content
}

// PrintJSONasEnv convert JSON file to env variables and print on stdout
func PrintJSONasEnv(jsonfile string) {
	file, _ := ioutil.ReadFile(jsonfile)
	c := NewConfiguration()
	_ = json.Unmarshal([]byte(file), c)
	// Print self node
	fmt.Println(ConfToString(c))
}

// Get a random string
func RandomString() string {
	ts := time.Now().UnixNano()
	s := rand.NewSource(ts)
	r := rand.New(s)
	ts = time.Now().UnixNano()
	rn := r.Int63n(ts)
	result := strconv.FormatInt(rn, 16)

	return result
}

func IntersectStringArrays(arr1 []string, arr2 []string) []string {
	if len(arr1) == 0 || len(arr2) == 0 {
		return nil
	}
	intersectionCache := make(map[string]bool)
	for _, str1 := range arr1 {
		intersectionCache[str1] = false
	}
	for _, str2 := range arr2 {
		if val, ok := intersectionCache[str2]; ok && !val {
			intersectionCache[str2] = true
		}
	}
	var result []string
	for key, val := range intersectionCache {
		if val {
			result = append(result, key)
		}
	}
	return result
}

func MergeStringArrays(arr1 []string, arr2 []string) []string {
	if len(arr1) == 0 && len(arr2) == 0 {
		return nil
	} else if len(arr1) == 0 {
		return arr2
	} else if len(arr2) == 0 {
		return arr1
	}

	intersectionCache := make(map[string]bool)
	result := []string{}
	for _, str1 := range arr1 {
		intersectionCache[str1] = true
		result = append(result, str1)
	}
	for _, str2 := range arr2 {
		if _, ok := intersectionCache[str2]; !ok {
			intersectionCache[str2] = true
			result = append(result, str2)
		}
	}
	return result
}
