package jadelet

import (
	"github.com/ant0ine/go-json-rest/rest"
	"net/http"
	"strconv"
	"uta.edu/aces/jade-go/kernel"
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
	j.log.Op.Println("querying node:", nodeName)
	node, err := j.Kube.FindNode(nodeName)
	if err == nil {
		w.WriteJson(node)
	} else {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ShowSubnodes show all subnodes registered
func (j *JADE) ShowSubnodes(w rest.ResponseWriter, r *rest.Request) {
	w.WriteJson(j.Subnodes)
}

// ShowSubnodes show all neighbors registered
func (j *JADE) ShowNeighbors(w rest.ResponseWriter, r *rest.Request) {
	w.WriteJson(j.Neighbors)
}

// ShowCapabilityCache will print all capabilities and their nodes
func (j *JADE) ShowCapabilityCacheSubnodes(w rest.ResponseWriter, r *rest.Request) {
	result := j.subnodeCapabilityCache.AllCapabilitiesWithNodes()
	w.WriteJson(result)
}

func (j *JADE) ShowCapabilityCacheNeighbors(w rest.ResponseWriter, r *rest.Request) {
	result := j.neighborCapabilityCache.AllCapabilitiesWithNodes()
	w.WriteJson(result)
}

// SearchNodes search subnodes according a list of capabilities
func (j *JADE) SearchSubnodes(w rest.ResponseWriter, r *rest.Request) {
	requirements := &kernel.Requirements{}
	err := r.DecodeJsonPayload(&requirements)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	nodes := j.selectAvaiableNodes(JadeNodeTypeSubnode, requirements)
	w.WriteJson(nodes)
}

// SearchNodes search neighbors according a list of capabilities
func (j *JADE) SearchNeighbors(w rest.ResponseWriter, r *rest.Request) {
	requirements := &kernel.Requirements{}
	err := r.DecodeJsonPayload(&requirements)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	nodes := j.selectAvaiableNodes(JadeNodeTypeNeighbor, requirements)
	w.WriteJson(nodes)
}

// ShowSubnodeCapacities show capacities of subnodes
func (j *JADE) ShowSubnodeCapacities(w rest.ResponseWriter, r *rest.Request) {
	result := make(map[string]map[string]*kernel.Capacity)
	result["remaining"] = make(map[string]*kernel.Capacity)
	result["maximum"] = make(map[string]*kernel.Capacity)
	for nodeID := range j.Subnodes {
		result["remaining"][nodeID] = j.subnodeCapacityCache.GetRemainingCapacity(nodeID)
		result["maximum"][nodeID] = j.subnodeCapacityCache.GetMaximumCapacity(nodeID)
	}
	w.WriteJson(result)
}

func (j *JADE) ShowTraces(w rest.ResponseWriter, r *rest.Request) {
	query := make(map[string]string)
	err := r.DecodeJsonPayload(&query)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	traceType := "concise"
	if t, e := query["type"]; e {
		traceType = t
	}
	if jobKey, e := query["jobId"]; e {
		j.log.Op.Printf("fetch traces for job[%v]", jobKey)
		w.WriteJson(j.TaskCache.CollectTraces(traceType, jobKey, j.log.Op.Printf))
	} else {
		j.log.Op.Printf("fetch traces for all jobs")
		w.WriteJson(j.TaskCache.CollectTraces(traceType, "", j.log.Op.Printf))
	}
}

func (j *JADE) ShowPerfEventsTraces(w rest.ResponseWriter, r *rest.Request) {
	query := make(map[string]string)
	err := r.DecodeJsonPayload(&query)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	traceType := "concise"
	if t, e := query["type"]; e {
		traceType = t
	}
	
	j.log.Op.Printf("fetch [%v] performance traces", traceType)
	w.WriteJson(j.PerfCache.PerfEventMatrices.CollectTraces(j.log.Op.Printf))
}

func (j *JADE) ShowTaskPerfTraces(w rest.ResponseWriter, r *rest.Request) {
	query := make(map[string]string)
	err := r.DecodeJsonPayload(&query)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	traceType := "concise"
	if t, e := query["type"]; e {
		traceType = t
	}
	
	j.log.Op.Printf("fetch [%v] performance traces", traceType)
	w.WriteJson(j.PerfCache.CollectTraces(traceType, j.log.Op.Printf))
}

func (j *JADE) ShowPodCache(w rest.ResponseWriter, r *rest.Request) {
	w.WriteJson(j.PodCache)
}

func (j *JADE) ShowTaskCache(w rest.ResponseWriter, r *rest.Request) {
	w.WriteJson(j.TaskCache.Describe())
}

func (j *JADE) GetJobIdList(w rest.ResponseWriter, r *rest.Request) {
	w.WriteJson(j.TaskCache.GetJobIdList())
}
