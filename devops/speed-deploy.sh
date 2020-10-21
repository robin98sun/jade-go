#!/usr/bin/env bash
master=$1
agent_base=$2
agent_count=$3
image=$4
delete=$5

if [[ "$delete" == "pods" || "$delete" == "all" ]];then
  kubectl get pods|sed '1d'|awk '{print $1}'|grep jade-local-test|xargs kubectl delete pods
fi

function master_service_name() {
  echo jade-local-test-${master}-service-external
}

function agent_service_name() {
  id=$1
  if [[ "$id" == "" ]];then
    id=1
  fi
  echo jade-local-test-${agent_base}${id}-service-external
}

if [[ "$delete" == "services" || "$delete" == "all" ]];then
  srvName=`master_service_name`
  kubectl delete service $srvName

  i=1
  while [[ $i -le $agent_count ]];do
    kubectl delete service `agent_service_name $i`
    i=`expr $i + 1`
  done
fi

# Deploy master first
./devops/k8s-deployer.py --print --namespace default --deployment-name jade-local-test \
    --application-name jadelet --application-image $image \
    --target-host ${master} --container-port 8080 \
    --env-var-file ./devops/deployments/local-test/env_variables-master.txt

node_list=${master}

# Deploy agents
i=1
while [[ $i -le $agent_count ]];do
  ./devops/k8s-deployer.py --print --namespace default --deployment-name jade-local-test \
    --application-name jadelet --application-image $image \
    --target-host ${agent_base}${i} --container-port 8080 \
    --env-var-file ./devops/deployments/local-test/env_variables-agent-${i}.txt 
  node_list="${node_list} ${agent_base}${i}"
  i=`expr $i + 1`
done

for node in ${node_list}; do
  port=`kubectl get service/jade-local-test-${node}-service-external --namespace default  --template='{{(index .spec.ports 0).nodePort}}'`
  echo $node $port
done