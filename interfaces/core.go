package interfaces

import (
	"aces/jade-go/conf"
	"fmt"
)

// JADE to instantiate JADE memory structure
type JADE struct {
	Config *conf.Conf
}

// Init to do initializing work
func (j *JADE) Init() {
	// read environment variables into config
	j.Config = conf.ReadConfFromEnv()
	fmt.Println("configurations from environment:")
	fmt.Println(j.Config)
	fmt.Println("")
}
