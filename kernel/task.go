package kernel

import (
// "math/rand"
// "strconv"
// "time"
)

type Task struct {
	Application  *Application  `json:"application,omitempty"`
	Requirements *Requirements `json:"requirements,omitempty"`
	Budget       *Budget       `json:"budget,omitempty"`
	Key          string        `json:"key,omitempty"`
}

func (t *Task) GenKey() {
	if t.Key != "" {
		return
	}
	t.Key = t.Application.Key() + ":" + RandomString()
}

func (t *Task) Valid() bool {
	if !t.Application.valid() {
		return false
	}
	if !t.Requirements.valid() {
		return false
	}
	if !t.Budget.valid() {
		return false
	}
	return true
}

// Budget the budget specification
type Budget struct {
	MaximumMilliseconds int `json:"maximumMilliseconds,omitempty"`
	Price               int `json:"price,omitempty"`
}

func (b *Budget) valid() bool {
	if b.MaximumMilliseconds == 0 || b.Price == 0 {
		return false
	}
	return true
}

// Application the application specficiations
type Application struct {
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

// Requirements the capabilities and resources requirements
type Requirements struct {
	Capabilities []*Capability `json:"capabilities,omitempty"`
	Allocations  *struct {
		Reducer *AllocationUnit `json:"reducer,omitempty"`
		Mapper  *AllocationUnit `json:"mapper,omitempty"`
	} `json:"allocations,omitempty"`
}

func (r *Requirements) valid() bool {
	if len(r.Capabilities) == 0 {
		return false
	}
	if r.Allocations == nil {
		return false
	}
	if r.Allocations.Reducer == nil {
		return false
	}
	if r.Allocations.Mapper == nil {
		return false
	}
	return true
}

// Sub-datastructures
// Container specify one container of an application
type Container struct {
	Registry   string                    `json:"registry,omitempty"`
	Interfaces map[string]*RESTInterface `json:"interfaces,omitempty"`
}

func (c *Container) valid() bool {
	if c.Registry == "" {
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
