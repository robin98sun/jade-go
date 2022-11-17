package provisioner

import (
	"strings"
<<<<<<< HEAD
	"uta.edu/aces/jade-go/kube"
	ds "uta.edu/aces/jadesdk/data_structure"
	"uta.edu/aces/jade-go/kernel"
=======
	"time"
	"uta.edu/aces/jade-go/kube"
	"uta.edu/aces/jade-go/kernel"
	ds "uta.edu/aces/jadesdk/data_structure"
>>>>>>> refactoring
)

type Provisioner struct {
	log *kernel.Logger
}

func NewProvisioner(logger *kernel.Logger) *Provisioner {
	return &Provisioner{
		log: logger,
	}
}

<<<<<<< HEAD
=======


>>>>>>> refactoring
func (p *Provisioner) ProvisionTask(client *kube.KubeClient, node *ds.Node,
	envVars []map[string]string, app *ds.Application,
	moduleName string, container *ds.Container,
	allocationLimits *ds.AllocationUnit,
<<<<<<< HEAD
	replicas int) (string, int, error) {
=======
	replicaIndex int, retryLimit int) (string, int, error) {
>>>>>>> refactoring
	// deploymentName
	deploymentName := purifyString(node.Hostname) +"-"+ purifyString(app.Name) 
	deploymentName += "-" + purifyString(app.Owner)
	// registry
	// Environment variables
	p.log.Op.Println("Provisioning pod", deploymentName, ", container image:", container.Image, ", conntainer port:", container.Port)
	deployedName, nodePort, err := client.ProvisionDeployment(
		app.EnvName, app.Owner,
		app.Name, app.Version, moduleName,
		// deploymentName, "k3s.io/hostname",
		deploymentName, "kubernetes.io/hostname",
		node.Hostname, node.Namespace,
		container.Image, container.Port,
		allocationLimits, envVars,
		replicaIndex,
	)


	if err != nil {
		retryDelay := 10
		p.log.Op.Println("Error when provisioning pods, deployment:", deploymentName, ", error:", err.Error())
		if retryLimit > 0 {
			p.log.Op.Println("going to retry in %v seconds", retryDelay)
			time.Sleep(time.Duration(retryDelay)*time.Second)
			return p.ProvisionTask(client, node, envVars, app, moduleName, container, allocationLimits, replicaIndex, retryLimit-1)
		} else {
			return deploymentName, 0, err
		}
	} else {
		p.log.Op.Println("Successfully provisioned pods, deployment:", deployedName, "nodePort:", nodePort)
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
