package provisioner

import (
	"aces/jade-go/kube"
	"fmt"
)

type Provision struct {
	Application struct {
		Image              string
		Version            string
		Port               int
		Name               string
		hasBeenProvisioned bool
	}
	Resource struct {
		Replicas  int
		Cpu       int
		Ram       int
		Disk      int
		Bandwidth int
	}
	Podname string
}

func (p *Provision) ProvisionApplication() string {
	p.Application.hasBeenProvisioned = true
	client := kube.KubeClient{}
	fmt.Println("provisioning app...")
	client.Init()
	return client.ProvisionStandardApp(&kube.AppSpec{
		Namespace:      "default",
		DeploymentName: "jade-dynamic",
		AppName:        p.Application.Name,
		Replicas:       2,
		ContainerName:  p.Application.Name,
		Image:          p.Application.Image + ":" + p.Application.Version,
		ContainerPort:  p.Application.Port,
		Protocol:       "TCP",
		InterfaceName:  "inf-" + p.Application.Name,
	})
}
