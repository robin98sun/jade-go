package kernel

import (
	"strings"
)

type Capability struct {
	Name       string   `json:"name,omitempty"`
	API        string   `json:"api,omitempty"`
	Type       string   `json:"type,omitempty"`
	Action     string   `json:"action,omitempty"`
	Value      string   `json:"value,omitempty"`
	URL        string   `json:"url,omitempty"`
	Parameters []*Param `json:"parameters,omitempty"`
}

type Param struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`
}

// Examples:
// {
// 	"name": "location",
// 	"api": ":static://NYC"
// },
// {
// 	"name": "accuracy",
// 	"api": ":static://city"
// },
// {
// 	"name": "avg_temperature",
// 	"api": "get:http://176.0.0.3/temperature:timespan=int"
// },
// {
// 	"name": "current_temperature",
// 	"api": "get:http://176.0.0.3/temperature"
// }

// NewCapability construct a capability instance with default values
func NewCapability() *Capability {
	return &Capability{
		Name: "",
		API:  "",
	}
}

// IsStatic tells whether a capability contain a static value
func (c *Capability) IsStatic() bool {
	if c.API != "" && c.API[0:9] == ":static://" {
		return true
	}
	return false
}

// MiniCapability generate a mini instance to transfer in the network
func (c *Capability) MiniCapability() *Capability {
	if c.Type == "" {
		return &Capability{
			Name: c.Name,
			API:  c.API,
		}
	} else if c.Type == "static" {
		return &Capability{
			Name:  c.Name,
			Value: c.Value,
		}
	} else {
		return &Capability{
			Name: c.Name,
		}
	}
}

// ParseAPI parse the API into structured data
func (c *Capability) ParseAPI() {
	if c.API == "" {
		return
	}
	parts := strings.Split(c.API, ":")
	if len(parts) < 3 {
		return
	}
	c.Type = parts[1]
	if c.Type == "static" {
		c.Value = parts[2]
		if len(c.Value) > 2 && c.Value[0:2] == "//" {
			c.Value = c.Value[2:]
		}
	} else {
		c.Action = parts[0]
		c.URL = parts[1] + ":" + parts[2]
		if len(parts) > 3 {
			params := parts[3]
			paramsParts := strings.Split(params, ",")
			for _, paramStr := range paramsParts {
				paramstrParts := strings.Split(paramStr, "=")
				if len(paramstrParts) == 2 {
					c.Parameters = append(c.Parameters, &Param{
						Name: paramstrParts[0],
						Type: paramstrParts[1],
					})
				}
			}
		}
	}
}
