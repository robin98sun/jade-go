// The main jadelet of JADE
// This basically is a http(rest) server, routing requests to sub-packages
package main

import (
	// RESTful Server
	"log"
	"net/http"

	"github.com/ant0ine/go-json-rest/rest"

	// Sub packages
	"aces/jade-go/jadelet"
	"aces/jade-go/kernel"

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
			kernel.PrintJSONasEnv(*configurationFileInJSON)
			os.Exit(0)
		}
	}
	// construt JADE RESTful API server
	j := jadelet.JADE{}
	j.Init()
	//

	api := rest.NewApi()
	api.Use(rest.DefaultDevStack...)
	router, err := rest.MakeRouter(
		// Control path upstream
		rest.Put("/$jade$/registerNode", j.RegisterNode),
		rest.Post("/$jade$/collectAcceptances", j.CollectAcceptances),
		// Control path downstream
		rest.Post("/$jade$/taskReceiver", j.TaskReceiver),
		// Data path upstream
		rest.Post("/$jade$/dataReceiver", j.DataReceiver),
		// for administration
		rest.Post("/$jade$/provision_app", j.ProvisionApp),
		rest.Put("/$jade$/configurations", j.UpdateConfigurations),
		// for debugging
		rest.Get("/$jade$/debug/jadelet", j.ShowJadelet),
		rest.Get("/$jade$/debug/configurations", j.ShowConfigurations),
		rest.Get("/$jade$/debug/pod", j.ShowPodInfo),
		rest.Get("/$jade$/debug/service", j.ShowService),
		rest.Get("/$jade$/debug/clusterIP", j.ShowClusterIP),
		rest.Get("/$jade$/debug/externalIP", j.ShowExternalIP),
		rest.Get("/$jade$/debug/node", j.ShowNode),
		rest.Get("/$jade$/debug/subnodes", j.ShowSubnodes),
		rest.Get("/$jade$/debug/taskCache", j.ShowTaskCache),
		rest.Get("/$jade$/debug/capabilityCache", j.ShowCapabilityCache),
		rest.Get("/$jade$/debug/capacityCache", j.ShowSubnodeCapacities),
		rest.Post("/$jade$/debug/searchNodes", j.SearchNodes),
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
