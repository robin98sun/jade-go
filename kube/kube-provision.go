package kube

import (
	"aces/jade-go/kernel"
	"context"
	"log"
	"strconv"

	// "k8s.io/apimachinery/pkg/api/errors"
	// appsv1 "k8s.io/api/apps/v1"
	// apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	// "k8s.io/client-go/dynamic"
	// "k8s.io/client-go/kubernetes"
	// "k8s.io/client-go/rest"
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

func (k *KubeClient) ProvisionPod(envName string, owner string, appname string, appversion string, podname string, hostnameKey string, hostname string, namespace string, image string, port int, allocation *kernel.AllocationUnit, capabilities []*kernel.Capability) (string, error) {
	environmentVariables := []map[string]string{}
	for _, c := range capabilities {
		environmentVariables = append(environmentVariables, map[string]string{
			"name":  c.Name,
			"value": c.API,
		})
	}
	labels := map[string]interface{}{
		"jade-env":         envName,
		"jade-role":        "application",
		"jade-owner":       owner,
		"jade-app":         appname,
		"jade-node":        hostname,
		"jade-app-version": appversion,
	}
	deploymentRes := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	deployment := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]interface{}{
				"name":      podname,
				"namespace": namespace,
				"labels":    labels,
			},
			"spec": map[string]interface{}{
				"replicas": 1,
				"selector": map[string]interface{}{
					"matchLabels": labels,
				},
				"template": map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": labels,
					},
					"spec": map[string]interface{}{
						"nodeSelector": map[string]interface{}{
							hostnameKey: hostname,
						},
						"containers": []map[string]interface{}{
							{
								"name":  appname,
								"image": image,
								"ports": []map[string]interface{}{
									{
										// "containerPort": "" + strconv.Itoa(port),
										"containerPort": port,
									},
								},
								"env": environmentVariables,
								"resources": map[string]interface{}{
									"requests": map[string]interface{}{
										"memory": strconv.FormatInt(allocation.MinimumCapacity.RAM, 10) + "Mi",
										"cpu":    strconv.FormatInt(allocation.MinimumCapacity.CPU, 10) + "m",
									},
									"limits": map[string]interface{}{
										"memory": strconv.FormatInt(allocation.MaximumCapacity.RAM, 10) + "Mi",
										"cpu":    strconv.FormatInt(allocation.MaximumCapacity.CPU, 10) + "m",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	log.Println("Deploying pod...")
	log.Println(deployment)
	result, err := k.Client.Resource(deploymentRes).Namespace(namespace).Create(context.TODO(), deployment, metav1.CreateOptions{})
	if err != nil {
		log.Println("ERROR while depolying pod:", err.Error())
		return "", err
	}
	podname = result.GetName()
	log.Printf("Completed deploying pod %q.\n", podname)
	return podname, nil

	// labels := map[string]string{
	// 	"jade-env":         envName,
	// 	"jade-role":        "application",
	// 	"jade-owner":       owner,
	// 	"jade-app":         appname,
	// 	"jade-node":        hostname,
	// 	"jade-app-version": appversion,
	// }

	// deploymentsClient := k.Clientset.AppsV1().Deployments(namespace)
	// deployment := &appsv1.Deployment{
	// 	ObjectMeta: metav1.ObjectMeta{
	// 		Name: podname,
	// 	},
	// 	Spec: appsv1.DeploymentSpec{
	// 		Replicas: int32Ptr(1),
	// 		// Selector: &metav1.LabelSelector{
	// 		// 	MatchLabels: labels,
	// 		// },
	// 		Template: apiv1.PodTemplateSpec{
	// 			ObjectMeta: metav1.ObjectMeta{
	// 				Labels: labels,
	// 			},
	// 			Spec: apiv1.PodSpec{
	// 				NodeSelector: map[string]string{
	// 					hostnameKey: hostname,
	// 				},
	// 				Containers: []apiv1.Container{
	// 					{
	// 						Name:  appname,
	// 						Image: image,
	// 						Ports: []apiv1.ContainerPort{
	// 							{
	// 								// Name:          "http",
	// 								Protocol:      apiv1.ProtocolTCP,
	// 								ContainerPort: int32(port),
	// 							},
	// 						},
	// 					},
	// 				},
	// 			},
	// 		},
	// 	},
	// }
	// deployment.Spec.Template.Spec.Containers[0].Resources.Limits[apiv1.ResourceCPU] = *resource.NewQuantity(allocation.MinimumCapacity.CPU, "")
	// deployment.Spec.Template.Spec.Containers[0].Resources.Limits[apiv1.ResourceMemory] = *resource.NewQuantity(allocation.MinimumCapacity.RAM, "")
	// deployment.Spec.Template.Spec.Containers[0].Resources.Requests[apiv1.ResourceCPU] = *resource.NewQuantity(allocation.MaximumCapacity.CPU, "")
	// deployment.Spec.Template.Spec.Containers[0].Resources.Requests[apiv1.ResourceMemory] = *resource.NewQuantity(allocation.MaximumCapacity.RAM, "")
	// log.Println("Deploying pod", podname)
	// result, err := deploymentsClient.Create(context.TODO(), deployment, metav1.CreateOptions{})
	// if err != nil {
	// 	log.Println("ERROR while depolying pod:", err.Error())
	// 	return "", err
	// }
	// deploymentName := result.GetObjectMeta().GetName()
	// log.Printf("Completed deploying pod %q.\n", deploymentName)
	// return deploymentName, nil
}

func int32Ptr(i int32) *int32 { return &i }
