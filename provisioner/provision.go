package provisioner

import (
	"strings"
	"uta.edu/aces/jade-go/kernel"
	"uta.edu/aces/jade-go/kube"
)

type Provisioner struct {
	log *kernel.Logger
}

func NewProvisioner(logger *kernel.Logger) *Provisioner {
	return &Provisioner{
		log: logger,
	}
}

func (p *Provisioner) ProvisionTask(client *kube.KubeClient, node *kernel.Node,
	envVars []map[string]string, app *kernel.Application,
	moduleName string, container *kernel.Container,
	allocationLimits *kernel.AllocationUnit,
	replicas int) (string, int, error) {
	// deploymentName
	deploymentName := purifyString(node.Hostname) + "-" + purifyString(app.Owner)
	deploymentName += "-" + purifyString(app.Name)
	// registry
	// Environment variables
	p.log.Println("Provisioning pod", deploymentName, ", container image:", container.Image, ", conntainer port:", container.Port)
	deployedName, nodePort, err := client.ProvisionDeployment(
		app.EnvName, app.Owner,
		app.Name, app.Version, moduleName,
		// deploymentName, "k3s.io/hostname",
		deploymentName, "kubernetes.io/hostname",
		node.Hostname, node.Namespace,
		container.Image, container.Port,
		allocationLimits, envVars, replicas,
	)
	if err != nil {
		p.log.Println("Error when provisioning pods, deployment:", deploymentName, ", error:", err.Error())
		return deploymentName, 0, err
	} else {
		p.log.Println("Successfully provisioned pods, deployment:", deployedName)
		return deployedName, nodePort, nil
	}
}

func purifyString(s string) string {
	str := s
	for _, ch := range []string{"_", ".", "/", ":"} {
		str = strings.ReplaceAll(str, ch, "-")
	}
	return str
}
