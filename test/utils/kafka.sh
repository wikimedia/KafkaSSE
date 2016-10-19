#!/bin/bash

KAFKA_VERSION="${KAFKA_VERSION:-0.9.0.1}"
SCALA_VERSION="${SCALA_VERSION:-2.11}"
KAFKA_HOME=${KAFKA_HOME:-"../kafka_${SCALA_VERSION}-${KAFKA_VERSION}"}

echo "Using kafka installed at ${KAFKA_HOME}"

if [ "$1" == "start" ]; then
    if [ `netstat -l | grep -q 2181; echo $?` -ne 0 ]; then
        echo "Starting Zookeeper..."
        echo "sh $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &"
        sh $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
        while [ `netstat -l | grep -q 2181; echo $?` -ne 0 ]; do
            echo "waiting for Zookeeper..."
            sleep 1 ;
        done
    else
        echo "Zookeper already running"
    fi

    if [ `netstat -l | grep -q 9092; echo $?` -ne 0 ]; then
        echo "Starting Kafka..."
        echo "sh $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &"
        sh $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &
        while [ `netstat -l | grep -q 9092; echo $?` -ne 0 ]; do
            echo "waiting for Kafka..." ;
            sleep 1 ;
        done
    else
        echo "Kafka already running";
    fi
elif [ "$1" == "stop" ]; then
    echo "Killing kafka and zookeeper and removing data"
    pkill -f '.*kafka.Kafka.*' --signal 9
    pkill -f '.*org.apache.zookeeper.server.quorum.QuorumPeerMain.*' --signal 9
    # cleanup kafka and zookeeper data
    rm -rfv /tmp/zookeeper
    rm -rfv /tmp/kafka-logs
fi
