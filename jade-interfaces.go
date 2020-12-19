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

	// APIs
	api := rest.NewApi()
	api.Use(rest.DefaultDevStack...)
	router, err := rest.MakeRouter(
		// Control path upstream
		rest.Put("/registerNode", j.RegisterNode),
		rest.Post("/collectProvisioning", j.CollectProvisioning),
		// Control path downstream
		rest.Post("/taskReceiver", j.TaskReceiver),
		// for administration
		rest.Put("/configurations", j.UpdateConfigurations),
		// for data path
		rest.Put("/app/listener", j.CollectAppMsg),
		rest.Get("/taskResults", j.GetAggregativeTaskResults),
		// for stat
		rest.Get("/dumpStat", j.DumpStat),
		// for debugging
		rest.Get("/debug/jadelet", j.ShowJadelet),
		rest.Get("/debug/configurations", j.ShowConfigurations),
		rest.Get("/debug/pod", j.ShowPodInfo),
		rest.Get("/debug/service", j.ShowService),
		rest.Get("/debug/clusterIP", j.ShowClusterIP),
		rest.Get("/debug/externalIP", j.ShowExternalIP),
		rest.Get("/debug/node", j.ShowNode),
		rest.Get("/debug/subnodes", j.ShowSubnodes),
		rest.Get("/debug/taskCache", j.ShowTaskCache),
		rest.Get("/debug/podCache", j.ShowPodCache),
		rest.Get("/debug/capabilityCache", j.ShowCapabilityCache),
		rest.Get("/debug/capacityCache", j.ShowSubnodeCapacities),
		rest.Post("/debug/searchNodes", j.SearchNodes),
	)
	if err != nil {
		log.Fatal(err)
	}
	api.SetApp(router)
	http.Handle("/$jade$/", http.StripPrefix("/$jade$", api.MakeHandler()))
	// UI
	http.Handle("/ui/", http.StripPrefix("/ui", http.FileServer(http.Dir("/ui"))))
	// Start HTTP server
	port := 8080
	fmt.Println("JADE is listening on port", port)
	// log.Fatal(http.ListenAndServe(":"+fmt.Sprint(port), api.MakeHandler()))
	log.Fatal(http.ListenAndServe(":"+fmt.Sprint(port), nil))
	fmt.Println("JADE is done")
}
