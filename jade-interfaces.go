// The main jadelet of JADE
// This basically is a http(rest) server, routing requests to sub-packages
package main

import (
	// RESTful Server
	"github.com/NYTimes/gziphandler"
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
	j.Verbose(false)
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
		rest.Delete("/taskCacheAndStat", j.ClearTaskCacheAndStat),
		rest.Delete("/podCache", j.ClearPodCache),
		rest.Get("/jobs", j.GetJobIdList),
		// for debugging
		rest.Get("/debug/jadelet", j.ShowJadelet),
		rest.Get("/debug/configurations", j.ShowConfigurations),
		rest.Get("/debug/pod", j.ShowPodInfo),
		rest.Get("/debug/service", j.ShowService),
		rest.Get("/debug/clusterIP", j.ShowClusterIP),
		rest.Get("/debug/externalIP", j.ShowExternalIP),
		rest.Get("/debug/node", j.ShowNode),
		rest.Get("/debug/subnodes", j.ShowSubnodes),
		rest.Post("/debug/collectTraces", j.ShowTraces),
		rest.Get("/debug/podCache", j.ShowPodCache),
		rest.Get("/debug/capabilityCache", j.ShowCapabilityCache),
		rest.Get("/debug/capacityCache", j.ShowSubnodeCapacities),
		rest.Post("/debug/searchNodes", j.SearchNodes),
	)
	if err != nil {
		log.Fatal(err)
	}
	api.SetApp(router)

	// Http server
	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// allow cross domain AJAX requests
			if origin := r.Header.Get("Origin"); origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				log.Printf("CORS origin: %v", origin)
			}
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Add("Access-Control-Max-Age", "86400")
			if r.Method == "OPTIONS" {
				// nothing need to do?
			} else {
				// business logic
				next.ServeHTTP(w, r)
			}
		})
	}
	http.Handle("/$jade$/", gziphandler.GzipHandler(middleware(http.StripPrefix("/$jade$", api.MakeHandler()))))
	// UI
	// http.Handle("/ui/", http.StripPrefix("/ui", http.FileServer(http.Dir("/ui"))))
	http.Handle("/", gziphandler.GzipHandler(http.FileServer(http.Dir("/ui"))))
	// http.Handle("/debug", gziphandler.GzipHandler(http.FileServer(http.Dir("/ui"))))
	// Start HTTP server
	port := 8080
	fmt.Println("JADE is listening on port", port)
	// log.Fatal(http.ListenAndServe(":"+fmt.Sprint(port), api.MakeHandler()))
	log.Fatal(http.ListenAndServe(":"+fmt.Sprint(port), nil))
	fmt.Println("JADE is done")
}
