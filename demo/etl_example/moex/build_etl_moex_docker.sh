#!/bin/bash

IMAGE_NAME=moex_etl


DOCKER_FILE_PATH="$(pwd)/Dockerfile"
cd ../../..

VERSION=$(grep -m1 -P -o "(?<=version\s=\s\")[0-9]+?\.[0-9]+?\.[0-9]+?(?=\")" ./pyproject.toml)

if docker images | grep -Eo "${IMAGE_NAME}\slatest"
then
    docker image remove ${IMAGE_NAME}:latest
fi
if docker images | grep -Eo "${IMAGE_NAME}\s${VERSION}"
then
    docker image remove ${IMAGE_NAME}:${VERSION}
fi

docker build --platform=linux/amd64 --target etl_moex -t ${IMAGE_NAME}:${VERSION} -t ${IMAGE_NAME}:latest -f ${DOCKER_FILE_PATH} .
