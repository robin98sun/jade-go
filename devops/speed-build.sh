#!/usr/bin/env bash
version=$1
jadelet_name=$2
registry=$3
registry_head="$registry"
if [[ "$jadelet_name" == "" ]];then
  jadelet_name=jadelet
fi
if [[ "$registry" != "" ]];then
  registry_head="$registry/"
fi

image_name="${registry_head}${jadelet_name}:$version"

rm -f app
go build -o app && \
docker image build --tag $image_name . && \
docker push $image_name

echo "${jadelet_name}@${version} has been built"
echo "image name:"
echo "$image_name"