'use strict';

/**
 * Collection of utility functions for KafkaSSE.
 */

const _                   = require('lodash');
const kafka               = require('node-rdkafka');
const P                   = require('bluebird');


/**
 * Converts a utf-8 byte buffer or a JSON string into
 * an object and returns it.
 */
function objectFactory(data) {
    // if we are given an object Object, no-op and return it now.
    if (_.isPlainObject(data)) {
        return data;
    }

    // If we are given a byte Buffer, parse it as utf-8
    if (data instanceof Buffer) {
        data = data.toString('utf-8');
    }

    // If we now have a string, then assume it is a JSON string.
    if (_.isString(data)) {
        data = JSON.parse(data);
    }

    else {
        throw new Error(
            'Could not convert data into an object.  ' +
            'Data must be a utf-8 byte buffer or a JSON string'
        );
    }

    return data;
}


/**
 * Returns a Promise of a promisified and connected rdkafka.KafkaConsumer.
 *
 * @param  {Object} kafkaConfig
 * @return {Promise<KafkaConsumer>} Promisified KafkaConsumer
 */
function createKafkaConsumerAsync(kafkaConfig) {
    let topicConfig = {};
    if ('default_topic_config' in kafkaConfig) {
        topicConfig = kafkaConfig.default_topic_config;
        delete kafkaConfig.default_topic_config;
    }

    const consumer = P.promisifyAll(
        new kafka.KafkaConsumer(kafkaConfig, topicConfig)
    );

    return consumer.connectAsync(undefined)
    .then((metadata) => {
        return consumer;
    });
}


/**
 * Return the intersection of existent topics and allowedTopics,
 * or just all existent topics if allowedTopics is undefined.
 *
 * @param  {Array}  topicsInfo topic metadata object, _metadata.topics.
 * @param  {Array}  allowedTopics topics allowed to be consumed.
 * @return {Array}  available topics
 */
function getAvailableTopics(topicsInfo, allowedTopics) {
    const existentTopics = topicsInfo.map(
        e => e.name
    )
    .filter(t => t !== '__consumer_offsets');

    if (allowedTopics) {
        return existentTopics.filter(t => _.includes(allowedTopics, t));
    }
    else {
        return existentTopics;
    }
}


/**
 * Checks assignments and makes sure it is either an Array of String
 * topic names, or it is an Array of plain objects containing
 * topic, partition, and offset properties.  If neither of these
 * conditions is met, this will throw an Error.  Otherwise returns true.
 *
 * @param  {Array} assignments
 * @throws {Error}
 * @return {Boolean}
 */
function validateAssignments(assignments) {
    // We will only throw this Error if assignments doesn't validate.
    // We pre-create it here just to DRY.
    let assignmentError = new Error(
        'Must provide either an array topic names, or ' +
        ' an array of objects with topic, partition and offset.'
    );
    assignmentError.assignments = assignments;

    if (!_.isArray(assignments) || _.isEmpty(assignments)) {
        throw assignmentError;
    }

    // Use the first element of this array will indicate the type of assignments.
    // If first element is string, assignments should be an Array of topic names.
    if (_.isString(assignments[0])) {
        if (!assignments.every((a) => _.isString(a))) {
            throw assignmentError;
        }
    }
    // Else assume this is an array of topic, partition, offset assignment objects.
    else {
        if (!assignments.every((a) => (
                _.isPlainObject(a)          &&
                _.isString(a.topic)         &&
                _.isInteger(a.partition)    &&
                _.isInteger(a.offset)
        ))) {
            throw assignmentError;
        }
    }

    return true;
}


/**
 * Given an Array of topics, this will return an array of
 * [{topic: t1, partition: 0, offset: -1}, ...]
 * for each topic-partition.  This is useful for manually passing
 * an to KafkaConsumer.assign, without actually subscribing
 * a consumer group with Kafka.
 *
 * @param  {Array}  topicsInfo topic metadata object, _metadata.topics.
 * @param  {Array}  topics we want to build partition assignments for.
 * @return {Array}  TopicPartition assignments starting at latest offset.
 */
function buildAssignments(topicsInfo, topics) {
    // Flatten results
    return _.flatten(
        // Find the topic metadata we want
        topicsInfo.filter(t => _.includes(topics, t.name))
        // Map them into topic, partition, offset: -1 (latest) assignment.
        .map(t => {
            return t.partitions.map(p => {
                return { topic: t.name, partition: p.id, offset: -1 };
            });
        })
    );
}


/**
 * Parses kafkaMessage.message as a JSON string and then
 * augments the object with kafka message metadata.
 * in the message._kafka sub object.
 *
 * @param  {KafkaMesssage} kafkaMessage
 * @return {Object}
 *
 */
function deserializeKafkaMessage(kafkaMessage) {
    kafkaMessage.message = objectFactory(kafkaMessage.value);

    kafkaMessage.message._kafka = {
        topic:     kafkaMessage.topic_name,
        partition: kafkaMessage.partition,
        offset:    kafkaMessage.offset,
        key:       kafkaMessage.key
    };

    return kafkaMessage;
}


/**
 * Serializes and sends an event to a connected SSE Client.
 *
 * @param {SSEClient}       sseClient
 * @param {String}          event
 * @param {Object|String}   data
 * @param {Object|String}   id
 */
function sseEmit(sseClient, event, data, id) {
    // Stringify the data and the id if it isn't already a string.
    if (!_.isString(data)) {
        data = JSON.stringify(data);
    }
    if (!_.isString(id)) {
        id = JSON.stringify(id);
    }

    // Send the event to the sse client/
    sseClient.send(event, data, id);
}


module.exports = {
    objectFactory:                  objectFactory,
    createKafkaConsumerAsync:       createKafkaConsumerAsync,
    getAvailableTopics:             getAvailableTopics,
    validateAssignments:            validateAssignments,
    buildAssignments:               buildAssignments,
    deserializeKafkaMessage:        deserializeKafkaMessage,
    sseEmit:                        sseEmit,
};
