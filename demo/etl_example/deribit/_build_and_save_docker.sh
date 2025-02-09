#!/bin/bash

source ./build_etl_deribit_docker.sh
IMAGE_NAME=deribit_etl
docker image save -o ./${IMAGE_NAME}.tar ${IMAGE_NAME}:latest
