#!/bin/bash

#KAFKA_VERSION="${KAFKA_VERSION:-0.9.0.1}"
#SCALA_VERSION="${SCALA_VERSION:-2.11}"
# This will only be used if KAFKA_TOPICS_CMD or
# KAFKA_CONSOLE_PRODUCER_CMD are not defined.
#KAFKA_HOME=${KAFKA_HOME:-"../kafka_${SCALA_VERSION}-${KAFKA_VERSION}"}


FIXTURES_DIR=${FIXTURES_DIR:-$(dirname $0)}
KAFKA_ZOOKEEPER_CONNECT=${KAFKA_ZOOKEEPER_CONNECT:-"127.0.0.1:2181"}
KAFKA_TOPICS_CMD=${KAFKA_TOPICS_CMD:-"$KAFKA_HOME/bin/kafka-topics.sh"}
KAFKA_CONSOLE_PRODUCER_CMD=${KAFKA_CONSOLE_PRODUCER_CMD:-"$KAFKA_HOME/bin/kafka-console-producer.sh"}


dropTopics ( ) {
  if [ "$#" -eq 1 ]
  then
    PATTERN=$1
    echo "looking for topics named '*${PATTERN}*'..."
    TOPICS=`${KAFKA_TOPICS_CMD} --zookeeper localhost:2181 --list \
    	| grep ${PATTERN} \
    	| grep -v 'marked for deletion$'`
    for TOPIC in ${TOPICS}; do
      echo "Dropping topic ${TOPIC}"
      ${KAFKA_TOPICS_CMD} --zookeeper ${KAFKA_ZOOKEEPER_CONNECT} --delete --topic ${TOPIC} > /dev/null &
    done
    wait
  fi
}

createTopic ( ) {
    echo "Creating topic ${1}"
    ${KAFKA_TOPICS_CMD} --create \
        --zookeeper ${KAFKA_ZOOKEEPER_CONNECT} \
        --partitions 1                         \
        --replication-factor 1                 \
        --topic $1 > /dev/null
}

produceTestData ( ) {
    echo "Producing ${2} into topic ${1}"
    ${KAFKA_CONSOLE_PRODUCER_CMD} --broker-list localhost:9092 --topic ${1} < ${2}
}

check ( ) {
  PORT=$1
  SERVICE_NAME=$2
  if [ `netstat -l | grep -q ${PORT}; echo $?` -ne 0 ]; then
    echo "${SERVICE_NAME} not running, start it first"
    exit 1
  fi
}

if [[ -z "$START_TIMEOUT" ]]; then
    START_TIMEOUT=600
fi

start_timeout_exceeded=false
count=0
step=10
while netstat -lnt | awk '$4 ~ /:'$KAFKA_PORT'$/ {exit 1}'; do
    echo "waiting for kafka to be ready"
    sleep $step;
    count=$(expr $count + $step)
    if [ $count -gt $START_TIMEOUT ]; then
        start_timeout_exceeded=true
        break
    fi
done

if $start_timeout_exceeded; then
    echo "Not able to load fixtures (waited for $START_TIMEOUT sec)"
    exit 1
fi

echo "Using kafka topics command: $KAFKA_TOPICS_CMD"
echo "Using kafka console producer command: $KAFKA_CONSOLE_PRODUCER_CMD"

#check 2181 "Zookeeper"
check 9092 "Kafka"
dropTopics "kafkaSSE_test_"
sleep 5

#  TODO: 0 index these topic names and data
(createTopic kafkaSSE_test_01 && produceTestData kafkaSSE_test_01 ${FIXTURES_DIR}/test_data1.json) &
(createTopic kafkaSSE_test_02 && produceTestData kafkaSSE_test_02 ${FIXTURES_DIR}/test_data2.json) &
(createTopic kafkaSSE_test_03 && produceTestData kafkaSSE_test_03 ${FIXTURES_DIR}/test_data3.json) &
(createTopic kafkaSSE_test_04 && produceTestData kafkaSSE_test_04 ${FIXTURES_DIR}/test_data2.json) &

wait
