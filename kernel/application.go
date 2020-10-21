package kernel

import (
	"strings"
	// "math/rand"
	// "strconv"
	// "time"
)

// Application the application specficiations
type Application struct {
	EnvName string     `json:"envName,omitempty"`
	Name    string     `json:"name,omitempty"`
	Version string     `json:"version,omitempty"`
	Owner   string     `json:"owner,omitempty"`
	Reducer *Container `json:"reducer,omitempty"`
	Mapper  *Container `json:"mapper,omitempty"`
}

func (a *Application) Key() string {
	return a.Owner + ":" + a.Name + ":" + a.Version
}

func (a *Application) valid() bool {
	if a.Name == "" || a.Version == "" || a.Owner == "" || a.Reducer == nil || a.Mapper == nil {
		return false
	}
	if a.Reducer.valid() {
		return a.Mapper.valid()
	} else {
		return false
	}
}

// Sub-datastructures
// Container specify one container of an application
type Container struct {
	Image      string                    `json:"image,omitempty"`
	Port       int                       `json:"port,omitempty"`
	Protocol   string                    `json:"protocol,omitempty"`
	Addr       string                    `json:"addr,omitempty"`
	Interfaces map[string]*RESTInterface `json:"interfaces,omitempty"`
}

func (c *Container) valid() bool {
	if c.Image == "" {
		return false
	}
	if c.Port == 0 {
		return false
	}
	if c.Protocol == "" || (strings.ToLower(c.Protocol) != "tcp" && strings.ToLower(c.Protocol) != "udp") {
		return false
	}
	if c.Interfaces != nil {
		for _, value := range c.Interfaces {
			if !value.valid() {
				return false
			}
		}
	}
	return true
}

// RESTInterface specify a RESTful interface
type RESTInterface struct {
	Port       int      `json:"port,omitempty"`
	Path       string   `json:"path,omitempty"`
	Method     string   `json:"method,omitempty"`
	Protocol   string   `json:"protocol,omitempty"`
	Parameters []*Param `json:"parameters,omitempty"`
}

func (i *RESTInterface) valid() bool {
	if i.Port == 0 || i.Path == "" || i.Method == "" || i.Protocol == "" || (i.Protocol != "http" && i.Protocol != "https") || (i.Parameters != nil && len(i.Parameters) == 0) {
		return false
	}
	return true
}
