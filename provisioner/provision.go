package provisioner

import (
	"aces/jade-go/kernel"
	"aces/jade-go/kube"
	"log"
	"strings"
)

type Provisioner struct {
}

func ProvisionTask(client *kube.KubeClient, node *kernel.Node, capabilities []*kernel.Capability, app *kernel.Application, container *kernel.Container, allocationLimits *kernel.AllocationUnit) (string, error) {
	// podname
	podname := purifyString(node.Hostname) + "-" + purifyString(app.Owner)
	podname += "-" + purifyString(app.Name) + "-" + purifyString(kernel.RandomString())
	// registry
	// Environment variables
	log.Println("Provisioning pod", podname)
	deploymentName, err := client.ProvisionPod(app.EnvName, app.Owner, app.Name, app.Version, podname, "k3s.io/hostname", node.Hostname, node.Namespace, container.Image, container.Port, allocationLimits, capabilities)
	if err != nil {
		log.Println("Error when provisioning pod", podname, ":", err.Error())
		return podname, err
	} else {
		log.Println("Successfully provisioned pod:", deploymentName)
		return deploymentName, nil
	}
}

func purifyString(s string) string {
	str := s
	for _, ch := range []string{"_", "."} {
		str = strings.ReplaceAll(str, ch, "-")
	}
	return str
}
