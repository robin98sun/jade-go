package kube

import (
	"context"
	// "fmt"

	// "k8s.io/apimachinery/pkg/api/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	// "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	// "k8s.io/apimachinery/pkg/runtime/schema"
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

// FindPod retrieve pods in a node and get specific instance according to the pod name
func (k *KubeClient) FindPod(nodeName string, namespace string, podName string) (corev1.Pod, error) {
	// pod, err := k.Clientset.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{
	// 	FieldSelector: "spec.nodeName=" + nodeName,
	// })
	pods, err := k.Clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	result := corev1.Pod{}
	if err == nil {
		for _, pod := range pods.Items {
			if pod.Name == podName {
				result = pod
				k.Pod = &pod
				break
			}
		}
	}
	return result, err
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
	if k.Pod == nil {
		k.FindPod(nodeName, namespace, podName)
	}
	if k.Pod != nil {
		return k.Pod.Status.PodIP
	}
	return ""
}
