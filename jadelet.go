// The main jadelet of JADE
// This basically is a http(rest) server, routing requests to sub-packages
package main

import (
	// RESTful Server
	"github.com/ant0ine/go-json-rest/rest"
	"log"
	"net/http"

	// Sub packages
	"uta.edu/aces/jade-go/jadelet"
	"uta.edu/aces/jade-go/kernel"

	// others
	"flag"
	"fmt"
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
	j := jadelet.NewJadelet()
	j.Init()
	j.Verbose(true)
	//

	api := rest.NewApi()
	api.Use(rest.DefaultDevStack...)
	router, err := rest.MakeRouter(
		// Control path upstream
		rest.Put("/$jade$/registerNode", j.RegisterNode),
		rest.Post("/$jade$/collectProvisioning", j.CollectProvisioning),
		// Control path downstream
		rest.Post("/$jade$/taskReceiver", j.TaskReceiver),
		// for administration
		rest.Put("/$jade$/configurations", j.UpdateConfigurations),
		// for data path
		rest.Put("/$jade$/app/listener", j.CollectAppMsg),
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
		rest.Get("/$jade$/debug/podCache", j.ShowPodCache),
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
