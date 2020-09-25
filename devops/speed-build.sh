#!/usr/bin/env bash
version=$1
rm -f app
go build -o app && \
docker image build --tag 192.168.57.8/jade:$version . && \
docker push 192.168.57.8/jade:$version