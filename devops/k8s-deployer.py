#!/usr/bin/env python3

# about how to use pyyaml package:
# https://stackabuse.com/reading-and-writing-yaml-to-a-file-in-python/
# pip install pyyaml
import yaml 

import argparse
import time
import os

parser = argparse.ArgumentParser(description='Deploy JADE in Kubernetes environment, including K8S and K3S')
parser.add_argument('--deployment-name', type=str, required=True,
                      help='the deployment name in Kubernetes cluster')
parser.add_argument('--application-name', type=str, required=True,
                      help='the name of the application')
parser.add_argument('--application-registry', type=str, required=True,
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
parser.add_argument('--bypass-deploying', action='store_true', help='bypass the actual deploying step')


args = parser.parse_args()
if args.deployment_file is not None:
  print("save in deployment file:", args.deployment_file)
print("namespace:", args.namespace)
print("deployment name:", args.deployment_name)
print("application name:", args.application_name)
print("application registry:", args.application_registry)
print("target host:", args.target_host)
if args.env_var_file is not None:
  print("environment variables file:", args.env_var_file)

doc = {
  "apiVersion": "v1",
  "kind": "Pod",
  "metadata": {
    "namespace": args.namespace,
    "name": args.deployment_name,
    "labels": {
      "jade-env": "local",
      "jade-role": "jadelet",
      "jade-owner": "jade"
    }
  },
  "spec": {
    "containers": [
      {
        "image": args.application_registry,
        "imagePullPolicy": "IfNotPresent",
        "name": args.application_name
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

if len(envVariables) > 0:
  doc["spec"]["containers"][0]["env"] = envVariables

content = yaml.dump(doc, default_flow_style=False)
# preprocess the file format
content = content.replace("!!##@@##!!", "")
# content = content.replace("'", '"')
if args.print:
  print("\ndeployment file content:\n")
  print(content)
  
filepath = "./tmp_jade_deployment."+ str(time.time()) +".yaml"
if args.deployment_file is not None:
  filepath = args.deployment_file

with open(filepath, 'w') as file:
  file.write(content)

if not args.bypass_deploying:
  os.system('sudo kubectl apply -f '+filepath)
if args.deployment_file is None:
  os.system('rm -f '+filepath)