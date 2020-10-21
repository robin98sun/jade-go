package provisioner

import (
	"aces/jade-go/kernel"
	"aces/jade-go/kube"
	"log"
	"strings"
)

type Provisioner struct {
}

func ProvisionTask(client *kube.KubeClient, node *kernel.Node,
	envVars []map[string]string, app *kernel.Application,
	moduleName string, container *kernel.Container,
	allocationLimits *kernel.AllocationUnit,
	replicas int) (string, int, error) {
	// deploymentName
	deploymentName := purifyString(node.Hostname) + "-" + purifyString(app.Owner)
	deploymentName += "-" + purifyString(app.Name)
	// registry
	// Environment variables
	log.Println("Provisioning pod", deploymentName)
	deploymentName, nodePort, err := client.ProvisionDeployment(
		app.EnvName, app.Owner,
		app.Name, app.Version, moduleName,
		deploymentName, "k3s.io/hostname",
		node.Hostname, node.Namespace,
		container.Image, container.Port,
		allocationLimits, envVars, replicas,
	)
	if err != nil {
		log.Println("Error when provisioning pods, deployment:", deploymentName, ", error:", err.Error())
		return deploymentName, 0, err
	} else {
		log.Println("Successfully provisioned pods, deployment:", deploymentName)
		return deploymentName, nodePort, nil
	}
}

func purifyString(s string) string {
	str := s
	for _, ch := range []string{"_", "."} {
		str = strings.ReplaceAll(str, ch, "-")
	}
	return str
}
