#!/usr/bin/env bash
version=$1
target=$2
specific=$3
if [[ "$target" == "pods" && "$specific" == "" ]];then
  sudo kubectl get pods|sed '1d'|awk '{print $1}'|xargs sudo kubectl delete pods
elif [[ "$target" == "pods" && "$specific" != "" ]];then
  sudo kubectl get pods|sed '1d'|grep "$specific"|awk '{print $1}'|xargs sudo kubectl delete pods
fi

if [[ "$target" == "services" && "$specific" == "" ]];then
  sudo kubectl delete service jade-local-test-edge-cluster-1-service-external && \
  sudo kubectl delete service jade-local-test-raspberry01-service-external
  sudo kubectl delete service jade-local-test-raspberry02-service-external
elif [[ "$target" == "services" && "$specific" == "" ]];then
  sudo kubectl delete service jade-local-test-${specific}-service-external
fi

if [[ "$target" == "" || "$target" == "services" || "$target" == "pods" ]]; then
  if [[ "$specific" == "" ]]; then
    ./devops/k8s-deployer.py --print --namespace default --deployment-name jade-local-test \
        --application-name jadelet --application-registry 192.168.57.8/jade:$version \
        --target-host edge-cluster-1 --container-port 8080 \
        --env-var-file env_variables-master.txt  && \
    ./devops/k8s-deployer.py --print --namespace default --deployment-name jade-local-test \
        --application-name jadelet --application-registry 192.168.57.8/jade:$version \
        --target-host raspberry01 --container-port 8080 \
        --env-var-file env_variables-agent-01.txt && \
    ./devops/k8s-deployer.py --print --namespace default --deployment-name jade-local-test \
        --application-name jadelet --application-registry 192.168.57.8/jade:$version \
        --target-host raspberry02 --container-port 8080 \
        --env-var-file env_variables-agent-02.txt 
  else 
    envfile=env_variables-master.txt
    if [[ "$specific" == "raspberry01" ]];then
      envfile=env_variables-agent-01.txt
    elif [[ "$specific" == "raspberry02" ]];then
      envfile=env_variables-agent-02.txt
    fi
    ./devops/k8s-deployer.py --print --namespace default --deployment-name jade-local-test \
        --application-name jadelet --application-registry 192.168.57.8/jade:$version \
        --target-host $specific --container-port 8080 \
        --env-var-file $envfile
  fi
fi