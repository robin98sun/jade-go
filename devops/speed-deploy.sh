#!/usr/bin/env bash
version=$1
target=$2
if [[ "$target" == "" || "$target" == "pods" ]];then
  sudo kubectl get pods|sed '1d'|awk '{print $1}'|xargs sudo kubectl delete pods
fi

if [[ "$target" == "services" ]];then
  sudo kubectl delete service jade-local-test-edge-cluster-1-service-external && \
  sudo kubectl delete service jade-local-test-raspberry02-service-external
fi

if [[ "$target" == "" || "$target" == "services" ]]; then
  ./devops/k8s-deployer.py --print --namespace default --deployment-name jade-local-test --application-name jadelet --application-registry 192.168.57.8/jade:$version --target-host raspberry02 --env-var-file env_variables-agent.txt --container-port 8080 && \
  ./devops/k8s-deployer.py --print --namespace default --deployment-name jade-local-test --application-name jadelet --application-registry 192.168.57.8/jade:$version --target-host edge-cluster-1 --env-var-file env_variables-master.txt --container-port 8080
fi