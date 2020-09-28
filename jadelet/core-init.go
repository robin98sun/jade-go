package jadelet

import (
	"aces/jade-go/kernel"
	"aces/jade-go/kube"
	// "bytes"
	// "encoding/json"
	// "errors"
	// "github.com/ant0ine/go-json-rest/rest"
	"log"
	// "net/http"
	// "time"
)

// Init to do initializing work
func (j *JADE) Init() {
	// Initialize caches and queues
	j.Subnodes = make(map[string]*kernel.Node)
	j.capabilityCache = &kernel.CapabilityCache{}
	j.capacityCache = &kernel.CapacityCache{}
	// read environment variables into config
	j.Config = kernel.ReadConfFromEnv()
	log.Println("configurations from environment:")
	log.Println("upper node:")
	log.Println(j.Config.UpperNode)
	log.Println("")
	log.Println("self node:")
	log.Println(j.Config.SelfNode)
	log.Println("")
	log.Println("capacity:")
	log.Println(j.Config.Capacity)
	log.Println("")
	log.Println("capabilities:")
	for _, c := range j.Config.Capabilities {
		log.Println(c)
	}
	log.Println("")
	// setup k8s client instance
	clients := kube.KubeClient{}
	clients.Init()
	j.Kube = &clients
	// Register to upper node
	go j.Register(0)
}
