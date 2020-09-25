package interfaces

import (
	// "aces/jade-go/conf"
	// "fmt"
	// "aces/jade-go/kube"
	"github.com/ant0ine/go-json-rest/rest"
	"log"
	"net/http"
	"strconv"
)

// ShowConfigurations show current JADE runtime configurations
func (j *JADE) ShowConfigurations(w rest.ResponseWriter, r *rest.Request) {
	w.WriteJson(j.Config)
}

// ShowPodInfo show current pod information
func (j *JADE) ShowPodInfo(w rest.ResponseWriter, r *rest.Request) {
	status, err := j.Kube.PodInfo(j.Config.SelfNode.Hostname, j.Config.SelfNode.Namespace, j.Config.SelfNode.PodName)
	if err == nil {
		w.WriteJson(status)
	} else {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ShowJadelet show current JADE instance in-memory data structure
func (j *JADE) ShowJadelet(w rest.ResponseWriter, r *rest.Request) {
	w.WriteJson(j.RegisterStatus)
}

// ShowClusterIP show clusterIP and targetPort of a service in a namespace
func (j *JADE) ShowClusterIP(w rest.ResponseWriter, r *rest.Request) {
	var serviceName string
	var namespace string
	for key, value := range r.URL.Query() {
		if key == "name" {
			serviceName = value[0]
		} else if key == "namespace" {
			namespace = value[0]
		}
	}
	clusterIP, targetPort, err := j.Kube.FindClusterIP(namespace, serviceName)
	if err == nil {
		w.WriteJson(clusterIP + ":" + strconv.Itoa(targetPort))
	} else {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ShowExternalIP show clusterIP and targetPort of a service in a namespace
func (j *JADE) ShowExternalIP(w rest.ResponseWriter, r *rest.Request) {
	var serviceName string
	var namespace string
	var nodeName string
	for key, value := range r.URL.Query() {
		if key == "service" {
			serviceName = value[0]
		} else if key == "namespace" {
			namespace = value[0]
		} else if key == "node" {
			nodeName = value[0]
		}
	}
	externalPort := j.Kube.FindExternalPort(namespace, serviceName)
	if externalPort != 0 {
		externalIP := j.Kube.FindExternalIP(nodeName)
		if externalIP != "" {
			w.WriteJson(externalIP + ":" + strconv.Itoa(externalPort))
			return
		}
	}
	rest.Error(w, "No external IP", http.StatusInternalServerError)
}

// ShowService show a service in a namespace
func (j *JADE) ShowService(w rest.ResponseWriter, r *rest.Request) {
	var serviceName string
	var namespace string
	for key, value := range r.URL.Query() {
		if key == "name" {
			serviceName = value[0]
		} else if key == "namespace" {
			namespace = value[0]
		}
	}
	service, err := j.Kube.FindService(namespace, serviceName)
	if err == nil {
		w.WriteJson(service)
	} else {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ShowNode show a node
func (j *JADE) ShowNode(w rest.ResponseWriter, r *rest.Request) {
	var nodeName string
	for key, value := range r.URL.Query() {
		if key == "name" {
			nodeName = value[0]
			break
		}
	}
	log.Println("querying node:", nodeName)
	node, err := j.Kube.FindNode(nodeName)
	if err == nil {
		w.WriteJson(node)
	} else {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
