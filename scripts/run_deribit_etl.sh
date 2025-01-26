#!/bin/bash

DOCKER_IMAGE=deribit_etl
DOCKER_NAME=deribit_etl
docker stop ${DOCKER_NAME}
docker rm ${DOCKER_NAME}

cd ..
if [ ! -d ./data ]; then
  mkdir -p ./data;
fi
cd ./data
DATA_PATH=$(pwd)
DOCKER_DATA_PATH=/workspace/data
docker run --name=${DOCKER_NAME} --rm -e ETL_TIMEFRAME=1h  -e DATA_PATH=${DOCKER_DATA_PATH} -v ${DATA_PATH}:${DOCKER_DATA_PATH} ${DOCKER_IMAGE}
#docker run --name=${DOCKER_NAME} -d -rm -e DATA_PATH=${DOCKER_DATA_PATH} -v ${DATA_PATH}:${DOCKER_DATA_PATH} ${DOCKER_IMAGE}
