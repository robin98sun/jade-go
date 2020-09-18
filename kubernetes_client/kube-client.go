package kubernetes_client

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
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
}

func (k *KubeClient) Init(isDynamic bool) {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	k.Config = config
	// creates the clientset
	if isDynamic {
		client, err := dynamic.NewForConfig(config)
		if err != nil {
			panic(err)
		}
		k.Client = client
	} else {
		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			panic(err.Error())
		}
		k.Clientset = clientset
	}
}

// List all pods for a given namespace
func (k *KubeClient) ListPods(namespace string) []string {
	// get pods in all the namespaces by omitting namespace
	// Or specify namespace to get pods in particular namespace
	pods, err := k.Clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		panic(err.Error())
	}
	result := make([]string, len(pods.Items))
	for i := 0; i < len(pods.Items); i++ {
		result[i] = pods.Items[i].Name
	}
	return result
}

func (k *KubeClient) FindPod(podname string) string {
	// get pods in all the namespaces by omitting namespace
	// Or specify namespace to get pods in particular namespace
	pods, err := k.Clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))

	// Examples for error handling:
	// - Use helper functions e.g. errors.IsNotFound()
	// - And/or cast to StatusError and use its properties like e.g. ErrStatus.Message
	_, err = k.Clientset.CoreV1().Pods("default").Get(context.TODO(), podname, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		fmt.Println("Pod ", podname, " not found in default namespace")
		return "NOT FOUND"
	} else if statusError, isStatus := err.(*errors.StatusError); isStatus {
		fmt.Printf("Error getting pod %v\n", statusError.ErrStatus.Message)
		return fmt.Sprintf("Error getting pod %v\n", statusError.ErrStatus.Message)
	} else if err != nil {
		panic(err.Error())
	} else {
		fmt.Printf("Found %v pod in default namespace\n", podname)
		return fmt.Sprintf("Found %v pod in default namespace\n", podname)
	}
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
