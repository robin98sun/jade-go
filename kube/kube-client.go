package kube

import (
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Logger interface {
	Println(...interface{})
	Printf(string, ...interface{})
}
type KubeClient struct {
	Clientset *kubernetes.Clientset
	Config    *rest.Config
	Client    dynamic.Interface
	log       Logger
}

func NewKubeClient(logger Logger) *KubeClient {
	return &KubeClient{
		log: logger,
	}
}

func (k *KubeClient) Init() {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	k.Config = config
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	k.Clientset = clientset
	// using dynamic
	client, errDyna := dynamic.NewForConfig(config)
	if err != nil {
		panic(errDyna.Error())
	}
	k.Client = client
}
