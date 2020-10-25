package kube

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FindPod retrieve pods in a node and get specific instance according to the pod name
func (k *KubeClient) FindPod(nodeName string, namespace string, podName string) (*corev1.Pod, error) {
	// pod, err := k.Clientset.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{
	// 	FieldSelector: "spec.nodeName=" + nodeName,
	// })
	pods, err := k.Clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err == nil {
		for _, pod := range pods.Items {
			if pod.Name == podName {
				return &pod, nil
			}
		}
	}
	return nil, err
}

// PodInfo retrieve pod instance and get its status
func (k *KubeClient) PodInfo(nodeName string, namespace string, podName string) (interface{}, error) {
	pod, err := k.FindPod(nodeName, namespace, podName)
	if err == nil {
		return pod.Status, nil
	}
	return nil, err
}

// PodIP retrieve pod instance and get its status
func (k *KubeClient) PodIP(nodeName string, namespace string, podName string) string {
	pod, err := k.FindPod(nodeName, namespace, podName)
	if pod != nil && err == nil {
		return pod.Status.PodIP
	}
	return ""
}

// FindService find service
func (k *KubeClient) FindService(namespace string, serviceName string) (*corev1.Service, error) {
	services, err := k.Clientset.CoreV1().Services(namespace).List(context.TODO(), metav1.ListOptions{
		// FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err == nil {
		for _, item := range services.Items {
			if item.Name == serviceName {
				return &item, nil
			}
		}
	}
	return nil, err
}

// FindClusterIP find service clusterIP and targetPort settings
func (k *KubeClient) FindClusterIP(namespace string, serviceName string) (string, int, error) {
	service, err := k.FindService(namespace, serviceName)
	if err == nil && service != nil {
		return service.Spec.ClusterIP, service.Spec.Ports[0].TargetPort.IntValue(), nil
	}
	return "", 0, err
}

// FindExternalPort find the port of service of external IP Address
func (k *KubeClient) FindExternalPort(namespace string, serviceName string) int {
	service, err := k.FindService(namespace, serviceName)
	if err == nil && service != nil {
		return int(service.Spec.Ports[0].NodePort)
	}
	return 0
}

// FindNode find service external IP Address and targetPort settings
func (k *KubeClient) FindNode(nodeName string) (*corev1.Node, error) {
	node, err := k.Clientset.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return node, err
}

// FindExternalIP get the external IP of a node
func (k *KubeClient) FindExternalIP(nodeName string) string {
	node, err := k.FindNode(nodeName)
	if err == nil {
		for _, addr := range node.Status.Addresses {
			if addr.Type == "ExternalIP" {
				return addr.Address
			}
		}
	}
	return ""
}
