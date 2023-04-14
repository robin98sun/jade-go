package kube

import (
	"context"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8s_labels "k8s.io/apimachinery/pkg/labels"
	"strconv"
	"strings"
	ds "uta.edu/aces/jadesdk/data_structure"
	"time"
)

func (k *KubeClient) ProvisionDeployment(envName string, owner string,
	appname string, appversion string, moduleName string,
	deploymentBaseName string, hostnameKey string, hostname string,
	namespace string, image string, port int,
	allocation *ds.AllocationUnit,
	envVars []map[string]string,
	replicaIndex int) (string, int, string, string, string, error) {

	replicaCount := 1

	environmentVariables := []map[string]string{}
	if envVars != nil {
		environmentVariables = envVars
	}

	// deploymentName can not longer than 63?
	deploymentName := deploymentBaseName
	if len(deploymentName) > 36 {
		deploymentName = deploymentName[0:36]
	}
	deploymentName =  strings.ToLower(deploymentName + "-" + ds.RandomString())

	labels := map[string]string{
		"jade-env":         strings.ReplaceAll(envName, "/", "-"),
		"jade-role":        "application",
		"jade-owner":       strings.ReplaceAll(owner, "/", "-"),
		"jade-app":         strings.ReplaceAll(appname, "/", "-"),
		"jade-node":        strings.ReplaceAll(hostname, "/", "-"),
		"jade-app-version": strings.ReplaceAll(appversion, "/", "-"),
		"jade-app-module":  strings.ReplaceAll(moduleName, "/", "-"),
		"jade-app-replica-index": "replica-"+strconv.Itoa(replicaIndex),
	}
	deploymentRes := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}

	var resourceMap map[string]interface{} = nil
	if allocation.MinimumCapacity.RAM > 0 || allocation.MaximumCapacity.RAM > 0 || allocation.MinimumCapacity.CPU > 0 || allocation.MaximumCapacity.CPU > 0 {
		resourceMap = map[string]interface{}{
			"requests": map[string]interface{}{
				"memory": strconv.FormatInt(allocation.MinimumCapacity.RAM, 10) + "Mi",
				"cpu": strconv.FormatInt(allocation.MinimumCapacity.CPU, 10) + "m",
			},
			"limits": map[string]interface{}{
				"memory": strconv.FormatInt(allocation.MaximumCapacity.RAM, 10) + "Mi",
				"cpu": strconv.FormatInt(allocation.MaximumCapacity.CPU, 10) + "m",
			},
		}
	}

	deploymentMap :=  map[string]interface{}{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]interface{}{
				"name":      deploymentName,
				"namespace": namespace,
				"labels":    labels,
			},
			"spec": map[string]interface{}{
				"replicas": replicaCount,
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
								"name":  strings.ReplaceAll(appname, "/", "-"),
								"image": image,
								"ports": []map[string]interface{}{
									{
										"containerPort": port,
									},
								},
								"env": environmentVariables,
								"resources": resourceMap,
							},
						},
					},
				},
			},
		}

	deployment := &unstructured.Unstructured{Object:deploymentMap}

	podUid := ""
	containerId := ""
	cgroupPath := ""
	k.log.Println("Deploying pods...")
	_, err := k.Client.Resource(deploymentRes).Namespace(namespace).Create(context.TODO(), deployment, metav1.CreateOptions{})
	if err != nil {
		k.log.Println("ERROR while depolying pods:", err.Error())
		return "", 0, podUid, containerId, cgroupPath, err
	} else {
		k.log.Println("Successfully deployed pod, wait 30 seconds to get pod UID and ContainerID")
		time.Sleep(time.Duration(30)*time.Second)
		// k.log.Println("the pods of the deployment "+deploymentName+":")
		// reference: https://itnext.io/generically-working-with-kubernetes-resources-in-go-53bce678f887

		labelSelectorString := k8s_labels.Set(
								metav1.LabelSelector{
									MatchLabels: labels,
								}.MatchLabels,
							).String()
		// k.log.Println("the label selector string: "+labelSelectorString)
		list, err := k.Clientset.CoreV1().Pods(namespace).List(
						context.Background(), 
						metav1.ListOptions{LabelSelector: labelSelectorString},
					)
		if err != nil {
			k.log.Println("ERROR while querying the pods information from K8s:")
			k.log.Println(err)
		} else if list != nil && len(list.Items) == 1 {
			// for podKey, item := range list.Items {
			// 	k.log.Printf("pod[%v] %+v\n\n", podKey, item.ObjectMeta)
			// 	k.log.Printf("%v Containers:\n", len(item.Spec.Containers))
			// 	for key, container := range item.Status.ContainerStatuses {
			// 		k.log.Printf("container[%v] %+v\n\n", key, container)
			// 	}
			// }
			// k.log.Println("END of the deployment information")

			pod := list.Items[0]
			podUid = string(pod.ObjectMeta.UID)
			if len(pod.Status.ContainerStatuses) != 1 {
				k.log.Printf("ERROR: the deployment deployed %v containers\n", len(list.Items))
			} else {
				container := pod.Status.ContainerStatuses[0]
				containerId = container.ContainerID
				k.log.Printf("The deployed pod UID: %v\n", podUid)
				k.log.Printf("The deployed container ID: %v\n", containerId)
			}

		} else if list != nil && len(list.Items) > 1 {
			k.log.Printf("ERROR: the deployment deployed %v pods\n", len(list.Items))

		} else {
			k.log.Println("ERROR: K8s returned empty response for the query")
		}

	}

	// resultBytes, _ := json.MarshalIndent(result, "", "  ")
	// k.log.Println("deployment:", deploymentName, ", result:", string(resultBytes))

	if podUid != "" && containerId != "" {
		parts := strings.Split(containerId, "://")
		if len(parts) == 2 {
			cgroupPath = "pod"+podUid + "/" + parts[1]
			if resourceMap != nil {
				cgroupPath = "kubepods/"+cgroupPath
			} else {
				cgroupPath = "kubepods/besteffort/"+cgroupPath
			}
		}
	}

	// deploy node port service for the pod
	nodePort, err := k.provisionNodePortService(deploymentName, namespace, labels, port)
	if err != nil {
		k.log.Println("ERROR while depolying node port services for pods:", err.Error())
		return "", 0, podUid, containerId, cgroupPath, err
	}

	k.log.Printf("Completed deploying pods, deployment name: %q.\n", deploymentName)
	return deploymentName, nodePort, podUid, containerId, cgroupPath, nil
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
