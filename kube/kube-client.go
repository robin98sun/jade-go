package kube

import (
	// "context"
	// "fmt"

	// "k8s.io/apimachinery/pkg/api/errors"
	// corev1 "k8s.io/api/core/v1"
	// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	// "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	// "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	//
	// Uncomment to load all auth plugins
	// _ "k8s.io/client-go/plugin/pkg/client/auth"
	//
	// Or uncomment to load specific auth plugins
	// _ "k8s.io/client-go/plugin/pkg/client/auth/azure"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/openstack"
)

type KubeClient struct {
	Clientset *kubernetes.Clientset
	Config    *rest.Config
	Client    dynamic.Interface
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
