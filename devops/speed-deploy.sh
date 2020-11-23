#!/usr/bin/env bash
node_name=$1
image=$2
env_file=$3
namespace=$4
delete=$5

if [[ "$env_file" == "" ]];then
  env_file="./jade-go/devops/deployments/lab/env/${node_name}.txt"
fi

if [[ "$namespace" == "" ]];then
  namespace=default
fi

if [[ "$delete" == "pods" || "$delete" == "all" ]];then
  kubectl get pods|sed '1d'|awk '{print $1}'|grep jadelet|xargs kubectl delete pods
fi

function service_name() {
  echo jadelet-${node_name}-service-external
}

if [[ "$delete" == "services" || "$delete" == "all" ]];then
  srvName=`service_name`
  kubectl delete service $srvName
fi

# Deploy agents
./jade-go/devops/k8s-deployer.py --print --namespace $namespace --deployment-name jadelet \
  --application-name jadelet --application-image $image \
  --target-host ${node_name} --container-port 8080 \
  --env-var-file ${env_file} 

port=`kubectl get service/jadelet-${node_name}-service-external --namespace $namespace  --template='{{(index .spec.ports 0).nodePort}}'`
echo $node_name $port  