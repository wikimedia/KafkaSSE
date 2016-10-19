#!/bin/bash

KAFKA_VERSION="${KAFKA_VERSION:-0.9.0.1}"
SCALA_VERSION="${SCALA_VERSION:-2.11}"
KAFKA_HOME=${KAFKA_HOME:-"../kafka_${SCALA_VERSION}-${KAFKA_VERSION}"}

if [ -d "${KAFKA_HOME}" ]; then
    if [ "${1}" == '-f' ]; then
        echo "Removing previously installed kafka at $KAFKA_HOME"
        rm -rf $KAFKA_HOME
    else
        echo "Kafka is already installed at $KAFKA_HOME"
        exit 0
    fi
fi

echo "Installing kafka at $KAFKA_HOME"

CXX=g++-4.8

wget http://www.us.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -O /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz
mkdir -pv ${KAFKA_HOME} && tar vxzf /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C ${KAFKA_HOME} --strip-components 1
echo 'delete.topic.enable=true' >> ${KAFKA_HOME}/config/server.properties

# Link installed kafka logs into jenkins workspace logs so that jenkins can pick them up.
if [ -n "${WORKSPACE}" ]; then
    ln -sfv "${WORKSPACE}/log" "${KAFKA_HOME}/logs"
else
    mkdir -pv "${KAFKA_HOME}/logs"
fi
