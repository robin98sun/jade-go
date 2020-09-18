// The main interfaces of JADE
// This basically is a http(rest) server, routing requests to sub-packages
package main

import (
	// RESTful Server
	"log"
	"net/http"

	"github.com/ant0ine/go-json-rest/rest"

	// Sub packages
	"aces/jade-go/conf"
	"aces/jade-go/provisioner"
	"aces/jade-go/query"

	// others
	"fmt"
	// "os"
	// "sync"
	"flag"
	"os"
)

func main() {
	// Read command line flags
	configurationFileInJSON := flag.String("config-json", "", "a configuration file in JSON format")
	printConfigVariables := flag.Bool("print-config-variables", false, "print configuration variables in stdout, then quit the program")
	flag.Parse()
	if *configurationFileInJSON != "" {
		if *printConfigVariables {
			conf.ConvertJSONToEnv(*configurationFileInJSON)
			os.Exit(0)
		}
	}
	// construt JADE RESTful API server
	j := JADE{}

	api := rest.NewApi()
	api.Use(rest.DefaultDevStack...)
	router, err := rest.MakeRouter(
		// Control path interfaces
		rest.Post("/$jade$/registerNode", j.RegisterNode),
		rest.Post("/$jade$/heartbeat", j.Heartbeat),
		// Data path interfaces
		rest.Post("/$jade$/taskReceiver", j.TaskReceiver),
		rest.Post("/$jade$/dataReceiver", j.DataReceiver),
		// for primitive test
		rest.Post("/query", j.Query),
		rest.Post("/provision_app", j.ProvisionApp),
	)
	if err != nil {
		log.Fatal(err)
	}
	api.SetApp(router)
	port := 8080
	fmt.Println("JADE is listening on port", port)
	log.Fatal(http.ListenAndServe(":"+fmt.Sprint(port), api.MakeHandler()))
	fmt.Println("JADE is done")
}

// to instantiate JADE memory structure
type JADE struct{}

// For primitive test
// Query interfaces
func (j *JADE) Query(w rest.ResponseWriter, r *rest.Request) {
	q := query.QueryData{}
	err := r.DecodeJsonPayload(&q)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	q.Query()
	w.WriteJson(&q)
}

// For primitive test
// Provisioning interfaces
func (j *JADE) ProvisionApp(w rest.ResponseWriter, r *rest.Request) {
	p := provisioner.Provision{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	podlist := p.ProvisionApplication()

	w.WriteJson(&podlist)
}

// Control path interfaces
// receive and process node registration
func (j *JADE) RegisterNode(w rest.ResponseWriter, r *rest.Request) {

}

// Heartbeat a
func (j *JADE) Heartbeat(w rest.ResponseWriter, r *rest.Request) {

}

// Data path interfaces
// task receiver
func (j *JADE) TaskReceiver(w rest.ResponseWriter, r *rest.Request) {

}

// Data receiver
func (j *JADE) DataReceiver(w rest.ResponseWriter, r *rest.Request) {

}
