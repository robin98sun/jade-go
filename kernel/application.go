package kernel

import (
	"strings"
)

// Application the application specficiations
type Application struct {
	EnvName string                `json:"envName,omitempty"`
	Name    string                `json:"name,omitempty"`
	Version string                `json:"version,omitempty"`
	Owner   string                `json:"owner,omitempty"`
	Modules map[string]*Container `json:"modules,omitempty"`
}

func (a *Application) GetModule(moduleName string) *Container {
	if len(a.Modules) == 0 {
		return nil
	}
	if m, exists := a.Modules[moduleName]; exists {
		return m
	} else {
		return nil
	}
}

func (a *Application) Key() string {
	return a.Owner + ":" + a.Name + ":" + a.Version
}

func (a *Application) valid() bool {
	if a.Name == "" || a.Version == "" || a.Owner == "" || len(a.Modules) == 0 {
		return false
	}
	valid := true
	for _, m := range a.Modules {
		if !m.valid() {
			valid = false
			break
		}
	}
	return valid
}

// Sub-datastructures
// Container specify one container of an application
type Container struct {
	Image    string      `json:"image,omitempty"`
	Port     int         `json:"port,omitempty"`
	Protocol string      `json:"protocol,omitempty"`
	Addr     string      `json:"addr,omitempty"`
	Input    interface{} `json:"input,omitempty"`
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
	return true
}

func (c *Container) SetISAInImage(isa string) {
	if isa == "" || c.Image == "" {
		return
	}

	separater := "--"
	parts := strings.Split(c.Image, separater)
	if len(parts) > 0 {
		new_image := ""
		targetPosition := 1
		if len(parts) > 1 {
			targetPosition = len(parts) - 1
		}
		for i:=0;i<targetPosition;i++ {
			new_image += parts[i] + separater
		}
		new_image += isa
		c.Image = new_image
	}
}

// predefined module names
type AppModule string

const (
	AppModuleAggregator AppModule = "aggregator"
	AppModuleWorker               = "worker"
)
