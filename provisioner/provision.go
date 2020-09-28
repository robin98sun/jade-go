package provisioner

import (
	"aces/jade-go/kernel"
	"aces/jade-go/kube"
	"log"
	"strings"
)

type Provisioner struct {
}

func ProvisionMapper(client *kube.KubeClient, node *kernel.Node, capabilities []*kernel.Capability, app *kernel.Application, allocationLimits *kernel.AllocationUnit) (string, error) {
	// podname
	podname := purifyString(node.Hostname) + "-" + purifyString(app.Owner)
	podname += "-" + purifyString(app.Name) + "-" + purifyString(kernel.RandomString())
	// registry
	// Environment variables
	log.Println("Provisioning pod", podname)
	realPodName, err := client.ProvisionPod(app.EnvName, app.Owner, app.Name, app.Version, podname, "k3s.io/hostname", node.Hostname, node.Namespace, app.Mapper.Image, app.Mapper.Port, allocationLimits, capabilities)
	if err != nil {
		log.Println("Error when provisioning pod", podname, ":", err.Error())
		return podname, err
	} else {
		log.Println("Successfully provisioned pod:", realPodName)
		return realPodName, nil
	}
}

func purifyString(s string) string {
	str := s
	for _, ch := range []string{"_", "."} {
		str = strings.ReplaceAll(str, ch, "-")
	}
	return str
}
