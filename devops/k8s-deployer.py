#!/usr/bin/env python3

# about how to use pyyaml package:
# https://stackabuse.com/reading-and-writing-yaml-to-a-file-in-python/
# pip install pyyaml
import yaml 

import argparse
import time
import os
import subprocess
import json
import sys

def evalcmd(cmd):
  print("CMD>> ",cmd)
  res = subprocess.check_output(cmd, shell=True)
  return res

parser = argparse.ArgumentParser(description='Deploy JADE in Kubernetes environment, including K8S and K3S')
parser.add_argument('--user', type=str, required=False,
                      help='the authorized in-cluster admin user name in Kubernetes cluster')
parser.add_argument('--deployment-name', type=str, required=True,
                      help='the deployment name in Kubernetes cluster')
parser.add_argument('--application-name', type=str, required=True,
                      help='the name of the application')
parser.add_argument('--application-image', type=str, required=True,
                      help='the doker registry of the application to deploy')
parser.add_argument('--deployment-file', type=str, required=False,
                      help='the deployment file to be used in `kubectl apply -f` command')
parser.add_argument('--target-host', type=str, required=True,
                      help='the target host name')
parser.add_argument('--namespace', type=str, required=False,
                      default="jade-app",
                      help='kubernetes namespace for JADE applications')
parser.add_argument('--env-var-file', type=str, required=False,
                      help='the file contains environment variables for the app')
parser.add_argument('--print', action='store_true', help='print the deployment file content')
parser.add_argument('--container-port', type=int, required=False, default=8080, help='container port of the application')
parser.add_argument('--bypass-deploying', action='store_true', help='bypass the actual deploying step')
parser.add_argument('--apply-json', type=str, required=False, help='using a JSON file for kubectl apply -f')
parser.add_argument('--fetch-node-port', type=bool, required=False, default=False, help='only fetch the nodePort of the desired pod')
args = parser.parse_args()

# Apply the deployment
def apply(obj, append=False):
  content = yaml.dump(obj, default_flow_style=False)
  # preprocess the file format
  content = content.replace("!!##@@##!!", "")
  # content = content.replace("'", '"')
  if args.print:
    print("\ndeployment file content:\n")
    print(content)
    
  filepath = "./tmp_jade_deployment."+ str(time.time()) +".yaml"
  if args.deployment_file is not None:
    filepath = args.deployment_file

  flag = 'w'
  if append: 
    flag = 'a'
  with open(filepath, flag) as file:
    file.write(content)

  if not args.bypass_deploying:
    os.system('kubectl apply -f '+filepath)
  if args.deployment_file is None:
    os.system('rm -f '+filepath)
  
  return filepath

# Apply json file
if args.apply_json is not None: 
  obj = json.load(args.apply_json)
  apply(obj)
  sys.exit()

# Get the token of the in-cluster admin user and set the context
# not workable for now, need further study
# default namespace and user is suggested
# reference: https://docs.cloud.oracle.com/en-us/iaas/Content/ContEng/Tasks/contengaddingserviceaccttoken.htm
# service_account = args.user
# token_name = evalcmd("sudo kubectl -n "+args.namespace+" get serviceaccount/"+ service_account +" -o jsonpath='{.secrets[0].name}'")
# token = evalcmd("sudo kubectl -n "+args.namespace+" get secret "+token_name+" -o jsonpath='{.data.token}'| base64 --decode")
# res = evalcmd("sudo kubectl config set-credentials "+ service_account +" --token="+token)
# print(res)
# res = evalcmd("sudo kubectl config set-context --current --user="+ service_account)
# print(res)

# Print other arguments
if args.deployment_file is not None:
  print("save in deployment file:", args.deployment_file)
print("namespace:", args.namespace)
print("deployment name:", args.deployment_name)
print("application name:", args.application_name)
print("application image:", args.application_image)
print("target host:", args.target_host)
if args.env_var_file is not None:
  print("environment variables file:", args.env_var_file)

podname = args.deployment_name + "-" + args.target_host
external_service_name = podname + "-service-external"
doc = {
  "apiVersion": "v1",
  "kind": "Pod",
  "metadata": {
    "namespace": args.namespace,
    "name": podname,
    "labels": {
      "jade-env": "local",
      "jade-role": "jadelet",
      "jade-owner": "jade",
      "jade-app": args.deployment_name,
      "jade-node": args.target_host,
    }
  },
  "spec": {
    "containers": [
      {
        "image": args.application_image,
        "imagePullPolicy": "IfNotPresent",
        "name": args.application_name,
        "ports": [
          {
            "containerPort": args.container_port
          }
        ]
      }
    ],
    "nodeSelector": {
      "k3s.io/hostname": args.target_host
    }
  }
}

envVariables = []
if args.env_var_file is not None:
  # parse environment variables file
  with open(args.env_var_file, 'r') as file:
    lines = file.readlines()
  for line in lines:
    line = line.strip()
    if line[0] == "#":
      continue
    parts = line.split("=", 1)
    if len(parts) < 2:
      continue
    var = {
      "name": parts[0],
      "value": "!!##@@##!!"+str(parts[1])+"!!##@@##!!"
    }
    envVariables.append(var)

#  overwrite self node information
self_node = [{
  "name": "JADE_SELFNODE_HOSTNAME",
  "value": args.target_host
}, {
  "name": "JADE_SELFNODE_PODNAME",
  "value": podname
}, {
  "name": "JADE_SELFNODE_NAMESPACE",
  "value": args.namespace
}, {
  "name": "JADE_SELFNODE_SERVICEEXTERNAL",
  "value": external_service_name
}]

for i in self_node:
  envVariables.append(i)

doc["spec"]["containers"][0]["env"] = envVariables


if not args.fetch_node_port:
  apply(doc)

# Create a service to allow external accessing of the 
doc = {
  "apiVersion": "v1",
  "kind": "Service",
  "metadata": {
    "namespace": args.namespace,
    "name": external_service_name,
    "labels": {
      "jade-env": "local",
      "jade-role": "jadelet",
      "jade-owner": "jade",
      "jade-app": args.deployment_name,
      "jade-node": args.target_host,
    }
  },
  "spec": {
    "selector": {
      # "jade-env": "local",
      # "jade-role": "jadelet",
      # "jade-owner": "jade",
      "jade-app": args.deployment_name,
      "jade-node": args.target_host,
    },
    "type": "NodePort",
    "ports": [
      {
        "protocol": "TCP",
        "port": args.container_port,
      }
    ]
  }
}

if not args.fetch_node_port:
  apply(doc, append=True)

# Get the runtime node port
# batcmd='sudo kubectl get service/'+ service_name +' --namespace '+ args.namespace +' --template=\'{{(index .spec.ports 0).nodePort}}{{"\\n"}}\''
batcmd='kubectl get service/'+ external_service_name +' --namespace '+ args.namespace +' --template=\'{{(index .spec.ports 0).nodePort}}\''
node_port = evalcmd(batcmd)
if not args.fetch_node_port:
  print("node port:", int(node_port))
  print("")
else:
  print(int(node_port))
