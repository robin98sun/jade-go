package kube

import (
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"log"
	"uta.edu/aces/jadesdk"
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
	jadesdk.TryCatchBlock{
		Try: func() {
			// creates the in-cluster config
			config, err := rest.InClusterConfig()
			if err != nil {
				// panic(err.Error())
				log.Println("can not initialize k8s client:", err.Error())
				return
			}
			k.Config = config
			// creates the clientset
			clientset, err := kubernetes.NewForConfig(config)
			if err != nil {
				// panic(err.Error())
				log.Println("can not create static config for k8s client:", err.Error())
				return
			}
			k.Clientset = clientset
			// using dynamic
			client, errDyna := dynamic.NewForConfig(config)
			if err != nil {
				// panic(errDyna.Error())
				log.Println("can not create dynamic config for k8s client:", err.Error())
				return
			}
			k.Client = client
		},
		Catch: func(e Exception) {
			log.Printf("ERROR when initializing k8s client: %v", e)
		},
	}.Do()
}
