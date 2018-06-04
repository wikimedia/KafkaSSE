FROM anapsix/alpine-java

MAINTAINER Wurstmeister

RUN apk add --update unzip wget curl jq coreutils

ENV KAFKA_VERSION="1.1.0" SCALA_VERSION="2.11"
ADD download_kafka.sh /tmp/download_kafka.sh
RUN chmod a+x /tmp/download_kafka.sh && sync && /tmp/download_kafka.sh && tar xfz /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt && rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka

VOLUME ["/kafka"]

ENV KAFKA_HOME /opt/kafka
ENV PATH ${PATH}:${KAFKA_HOME}/bin
ENV FIXTURES_DIR /tmp/fixtures
ADD start_kafka.sh /usr/bin/start_kafka.sh
ADD load_fixtures.sh /usr/bin/load_fixtures.sh
ADD test_data1.json ${FIXTURES_DIR}/test_data1.json
ADD test_data2.json ${FIXTURES_DIR}/test_data2.json
ADD test_data3.json ${FIXTURES_DIR}/test_data3.json

# The scripts need to have executable permission
RUN chmod a+x /usr/bin/start_kafka.sh && \
    chmod a+x /usr/bin/load_fixtures.sh

EXPOSE 9092

# Use "exec" form so that it runs as PID 1 (useful for graceful shutdown)
CMD ["start_kafka.sh"]
