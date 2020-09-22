package kube

import (
	"context"
	"fmt"

	// "k8s.io/apimachinery/pkg/api/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	Pod       *corev1.Pod
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

type AppSpec struct {
	Namespace      string
	DeploymentName string
	AppName        string
	Replicas       int
	ContainerName  string
	Image          string
	ContainerPort  int
	Protocol       string
	InterfaceName  string
}

func (k *KubeClient) ProvisionStandardApp(app *AppSpec) string {

	deploymentRes := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}

	deployment := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]interface{}{
				"name": app.DeploymentName,
			},
			"spec": map[string]interface{}{
				"replicas": 2,
				"selector": map[string]interface{}{
					"matchLabels": map[string]interface{}{
						"app": app.AppName,
					},
				},
				"template": map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": app.AppName,
						},
					},

					"spec": map[string]interface{}{
						"containers": []map[string]interface{}{
							{
								"name":  app.ContainerName,
								"image": app.Image,
								"ports": []map[string]interface{}{
									{
										"name":          app.InterfaceName,
										"protocol":      app.Protocol,
										"containerPort": app.ContainerPort,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Create Deployment
	fmt.Println("Creating deployment...")
	result, err := k.Client.Resource(deploymentRes).Namespace(app.Namespace).Create(context.TODO(), deployment, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created deployment %q.\n", result.GetName())
	return fmt.Sprintf("Created deployment %q.\n", result.GetName())
}
