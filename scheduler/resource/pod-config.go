package resource

import (
	// "time"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jadesdk"
	// "fmt"
	// "strings"
)


type PodConfig struct {
	Node  		*jadesdk.Node `json:"node,omitempty"`
	Application *kernel.Application `json:"application,omitempty"`
	Pod         *kernel.Pod `json:"pod,omitempty"`
	Options		*PodConfigOptions `json:"options,omitempty"`
}

type PodConfigOptions struct {
	IsInitialization    bool `json:"isInitialization,omitempty"`
	HasProvisioned      bool `json:"hasProvisioned,omitempty"`
	SecondsToFail       int  `json:"secondsToFail,omitempty"`
	ReprovisionIfFail	bool `json:"provisionIfFail,omitempty"`
	PropagateToSubnodes bool `json:"propagateToSubnodes,omitempty"`
}
