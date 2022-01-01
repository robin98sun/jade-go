package kube

import (
	"context"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"strconv"
	"strings"
	"uta.edu/aces/jade-go/kernel"
)

func (k *KubeClient) ProvisionDeployment(envName string, owner string,
	appname string, appversion string, moduleName string,
	deploymentBaseName string, hostnameKey string, hostname string,
	namespace string, image string, port int,
	allocation *kernel.AllocationUnit,
	envVars []map[string]string,
	replicas int) (string, int, error) {
	environmentVariables := []map[string]string{}
	if envVars != nil {
		environmentVariables = envVars
	}

	// deploymentName can not longer than 63?
	deploymentName := deploymentBaseName
	if len(deploymentName) > 24 {
		deploymentName = deploymentName[0:24]
	}
	deploymentName = "app-jade-" + deploymentName + "-" + kernel.RandomString()

	labels := map[string]string{
		"jade-env":         envName,
		"jade-role":        "application",
		"jade-owner":       owner,
		"jade-app":         strings.ReplaceAll(appname, "/", "-"),
		"jade-node":        hostname,
		"jade-app-version": appversion,
		"jade-app-module":  moduleName,
	}
	deploymentRes := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	deployment := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]interface{}{
				"name":      deploymentName,
				"namespace": namespace,
				"labels":    labels,
			},
			"spec": map[string]interface{}{
				"replicas": replicas,
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

	k.log.Println("Deploying pods...")
	_, err := k.Client.Resource(deploymentRes).Namespace(namespace).Create(context.TODO(), deployment, metav1.CreateOptions{})
	if err != nil {
		k.log.Println("ERROR while depolying pods:", err.Error())
		return "", 0, err
	}
	// resultBytes, _ := json.MarshalIndent(result, "", "  ")
	// k.log.Println("deployment:", deploymentName, ",result:", string(resultBytes))

	// deploy node port service for the pod
	nodePort, err := k.provisionNodePortService(deploymentName, namespace, labels, port)
	if err != nil {
		k.log.Println("ERROR while depolying node port services for pods:", err.Error())
		return "", 0, err
	}

	k.log.Printf("Completed deploying pods, deployment name: %q.\n", deploymentName)
	return deploymentName, nodePort, nil
}

func int32Ptr(i int32) *int32 { return &i }

func (k *KubeClient) provisionNodePortService(
	deploymentName string,
	namespace string,
	labels map[string]string,
	containerPort int,
) (int, error) {
	// service name can not longer than 63
	serviceName := "srv-" + deploymentName
	k.log.Println("Deploying node port service for pods...")

	// https://stackoverflow.com/questions/53874921/kubernetes-client-go-creating-services-and-enpdoints
	_, err := k.Clientset.CoreV1().Services(namespace).Create(context.TODO(), &apiv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: apiv1.ServiceSpec{
			Selector: labels,
			Type:     "NodePort",
			Ports: []apiv1.ServicePort{
				apiv1.ServicePort{
					Protocol: "TCP",
					Port:     int32(containerPort),
				},
			},
		},
	}, metav1.CreateOptions{})

	if err != nil {
		k.log.Println("ERROR while depolying node port service for pods:", err.Error())
		return 0, err
	}
	// resultBytes, _ := json.MarshalIndent(result, "", "  ")
	// k.log.Println("node port service deployment result:", string(resultBytes))
	nodePort := k.FindExternalPort(namespace, serviceName)
	k.log.Println("deployment:", deploymentName, ", service:", serviceName, ", node port:", nodePort)
	k.log.Printf("Completed deploying node port service for pods, service name: %q, deployment: %q.\n", serviceName, deploymentName)
	return nodePort, nil
}
