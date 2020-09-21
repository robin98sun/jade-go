// The main interfaces of JADE
// This basically is a http(rest) server, routing requests to sub-packages
package main

import (
	// RESTful Server
	"log"
	"net/http"

	"github.com/ant0ine/go-json-rest/rest"

	// Sub packages
	"aces/jade-go/interfaces"

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
			conf.PrintJSONasEnv(*configurationFileInJSON)
			os.Exit(0)
		}
	}
	// construt JADE RESTful API server
	j := interfaces.JADE{}
	j.Init()
	//

	api := rest.NewApi()
	api.Use(rest.DefaultDevStack...)
	router, err := rest.MakeRouter(
		// Control path interfaces
		rest.Post("/$jade$/registerNode", j.RegisterNode),
		rest.Post("/$jade$/heartbeat", j.Heartbeat),
		// Data path interfaces
		rest.Post("/$jade$/taskReceiver", j.TaskReceiver),
		rest.Post("/$jade$/dataReceiver", j.DataReceiver),
		// for administration
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
