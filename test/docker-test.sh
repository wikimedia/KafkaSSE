#!/bin/bash

# This code has been tested with:
# - docker-engine^1.12.4
# - docker-compose^1.9.0
# Given it uses version 2 of docker-compose files,
# it should not be compatible with earlier versions.

# Network name configured in docker file:
APP_NETWORK="kafka_net"

# Internally used variables
APP_NAME="testkafkasse"
DOCKER_COMPOSE_CMD="docker-compose -p ${APP_NAME}"
IMAGE_NAME="testkafkasse"

# Initialised to empty
KAFKA_CONTAINERS=""


# Build and start kafka/zookeeper containers
start_kafka() {
    echo "Building and starting kafka/zookeeper containers."
    pushd docker > /dev/null
    ${DOCKER_COMPOSE_CMD} build
    ${DOCKER_COMPOSE_CMD} up -d
    popd > /dev/null

    # Check kafka container is running
    KAFKA_CONTAINERS=$(docker ps | grep "9092/tcp")
    if [ -z ${KAFKA_CONTAINERS} ]; then
        echo "No kafka container running - Stopping in error!" && exit 1
    else
        echo "Kafka container running - Moving forward!"
    fi
}

stop_kafka() {
    echo "Stopping kafka/zookeeper containers (if any)."
    pushd docker > /dev/null
    ${DOCKER_COMPOSE_CMD} stop
    ${DOCKER_COMPOSE_CMD} rm -f
    popd > /dev/null

    KAFKA_CONTAINERS=$(docker ps | grep "9092/tcp")
    if [ -z ${KAFKA_CONTAINERS} ]; then
        echo "No kafka container running - Good!"
    else
        echo "Kafka container still running - Not good!" && exit 1
    fi
}


build_and_test() {
    # force rebuild testKafkaSSE image and run it (execute tests)
    echo "Building ${IMAGE_NAME} docker image."
    pushd ../ > /dev/null
    docker build --no-cache -t test_kafkasse .
    echo "Executing ${IMAGE_NAME} docker image (run tests)."
    TEST_SUCCESS=$(docker run --rm --net ${APP_NAME}_${APP_NETWORK} ${IMAGE_NAME} | grep "npm ERR!")
    echo ${TEST_SUCCESS}
    popd > /dev/null

}

check_test() {
    if [ -z ${TEST_SUCCESS} ]; then
        echo "Tests successful !" && exit 0
    else
        echo "Tests NOT successful :(" && exit 1
    fi
}


stop_kafka

start_kafka

# Give some time to containers to start
sleep 5

build_and_test

stop_kafka

check_test
