package kernel

type Pod struct {
	Namespace string `json:"namespace"`
	PodName   string `json:"podName"`
	ClusterIP string `json:"clusterIP"`
	PodIP     string `json:"podIP"`
	HostIP    int    `json:"hostIP"`
}
