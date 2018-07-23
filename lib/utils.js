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
    .thenReturn(consumer);
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
 * topic, partition, and offset|timestamp properties.  If neither of these
 * conditions is met, this will throw an Error.  Otherwise returns true.
 * This validates that assignments are suitable for use in KafkaSSE, NOT
 * for use by KafkaConsumer.assign().  First pass these assignments through
 * buildAssignmentsAsync().
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
        ' an array of objects with topic, partition and offset|timestamp.'
    );
    assignmentError.assignments = assignments;

    if (!_.isArray(assignments) || _.isEmpty(assignments)) {
        throw assignmentError;
    }

    // Every assignement must either be...
    if (!assignments.every((a) => (
            // a string topic name
            _.isString(a) || (
            // or a an assignment object
                _.isPlainObject(a)          &&
                _.isString(a.topic)         &&
                _.isInteger(a.partition)    &&
                (_.isInteger(a.offset) || _.isInteger(a.timestamp))
            )
    ))) {
        throw assignmentError;
    }

    return true;
}


/**
 * Given topic names, this will create TopicPartition assignment objects
 * for every partition in each topic, with either timestamp: atTimestamp
 * or offset: defaultOffset.
 *
 * @param  {Array}  topicsInfo  topic metadata object, _metadata.topics.
 * @param  {Array|String} of topics to build TopicPartition assignment objects for.
 * @param  {int}  atTimestamp   If given, the assignment will have timestamp: atTimestamp set
 * @param  {int}  defaultOffset If atTimestamp is not given, the assignment will have
 *                               offset: defaultOffset set.  Default: -1 (latest offset)
 * @return {Array} TopicPartition assignments, suitable for passing to buildAssignmentsAsync.
 *                 If atTimestamp is given, the returned array SHOULD NOT be used to
 *                 pass to KafkaConsumer.assign.  First resolve the timestamps into
 *                 offsets by passing the assignments through buildAssignmentsAsync().
 */
function topicsToPartitionAssignment(topicsInfo, topics, atTimestamp, defaultOffset = -1) {
    topics = _.isArray(topics) ? topics : topics.split(',');

    // This will be merged into the partition assignment, to either have the assignment
    // start from atTimestamp if given, else defaultOffset.
    const defaultResumePosition = atTimestamp ? {timestamp: atTimestamp} : {offset: defaultOffset};

    // Find the topic-partition metadata we want
    return _.flatten(
        topicsInfo.filter(t => _.includes(topics, t.name))
        // Map them into topic, partition, merging in the
        // defaultResumePosition of either timestamp or offset.
        .map(
            t => t.partitions.map(
                p => Object.assign({topic: t.name, partition: p.id}, defaultResumePosition)
            )
        )
    );
}


/**
 * Given an Array of assignments, this will do the following:
 *
 *  - String entries are assumed to be topic names, and will be
 *    converted to partition assignment objects
 *    starting from either atTimestamp, or at -1 (latest).
 *
 *  - Object entries with offset already set will be left as they are.
 *
 *  - Object entries without offset but with timestamp will query offsetsForTimes
 *    to convert the timestamp to the offset for the given timestamp.
 *
 *  - Object entries without offset or timestamp will start from the offset
 *    returned by offsetsForTimes at atTimestamp, or defaultOffset if
 *    atTimestamp is not given.
 *
 * The returned Array will then contain assignment entry objects of the form:
 *  [{topic: t1, partition: 0, offset: -1}, ...]
 * for each topic-partition.  This Array can be passed to
 * KafkaConsumer.assign, without actually 'subscribing'
 * a consumer group with Kafka.
 *
 * @param  {Promise<KafkaConsumer>} Promisified and connected KafkaConsumer
 * @param  {Array}  assignments of string topic names or assignment objects
 *                  with either offset or timestamp set.
 * @param  {int}    atTimestamp.  If given, will be used to find offsets for assignments
 *                  without offset or timestamp already set.
 * @param  {int}    defaultOffset.  If atTimestamp is not given, this will be used
 *                  for any assigment for which offset|timestamp is not already set.
 *                  Default: -1 (latest offset).
 * @return {Array}  TopicPartition assignments
 */
function buildAssignmentsAsync(kafkaConsumer, assignments, atTimestamp, defaultOffset = -1) {
    // This will be merged into the partition assignment that don't already have either
    // timestamp or offset set, to either have the assignment start from atTimestamp if given,
    // else defaultOffset.
    const defaultResumePosition = atTimestamp ? {timestamp: atTimestamp} : {offset: defaultOffset};

    // Convert any string topic assignment entries to assignment objects
    // including all partitions.
    assignments = _.flatten(
        assignments.map((a) => {
            // If this assignment was given as a string topic, convert it to a list
            // of all topic partition assignments for the topic starting from
            // atTimestamp|defaultOffset.
            if (_.isString(a)) {
                a = topicsToPartitionAssignment(
                    kafkaConsumer._metadata.topics, a, atTimestamp, defaultOffset
                );
            }
            // Else this is an assignment object.
            // Make sure it has either an offset or a timestamp.
            // If not, merge in defaultResumePosition to set one.
            else if (!('offset' in a) && !('timestamp') in a) {
                a = Object.assign(a, defaultResumePosition);
            }
            return a;
        })
    );

    // We are now sure we have an array of assignment objects.
    // For any that don't have an offset, but have a timestamp set, we need
    // to query offsetsForTimes to convert the timestamps to offsets.
    return assignmentsForTimesAsync(kafkaConsumer, assignments);
}


/**
 * Given a a Kafka assignemnts array, this will look for any occurances
 * of 'timestamp' instead of 'offset'.  For those found, it will issue
 * a offsetsForTimes request on the kafkaConsumer.  The returned
 * offsets will be merged with any provided non-timestamp based assignments,
 * and returned as a Promise of assignments array with only offsets.
 *
 * If no timestamps based assignments are given, this will just return a Promise
 * of the provided assignments.
 *
 * @param  {KafkaConsumer} kafkaConsumer
 * @param  {Array}         assignments
 * @return {Promise}       Array of timestamp resolved assignment objects with offsets.
 */
function assignmentsForTimesAsync(kafkaConsumer, assignments) {
    const assignmentsWithTimestamps = assignments.filter(a => 'timestamp' in a && !('offset' in a)).map((a) => {
        return {topic: a.topic, partition: a.partition, offset: a.timestamp };
    });

    // If there were no timestamps to resolve, just return assignments as is.
    if (assignmentsWithTimestamps.length == 0) {
        return new P((resolve, reject) => resolve(assignments));
    }
    // Else resolve all timestamp assignments to offsets.
    else {
        // Get offsets for the timestamp based assignments.
        // If no offset was found for any timestamp, this will return -1, which
        // we can use for offset: -1 as end of partition.
        return kafkaConsumer.offsetsForTimesAsync(assignmentsWithTimestamps)
        .then(offsetsForTimes => {
            // console.log({ withTimestamps: assignmentsWithTimestamps, got: offsetsForTimes}, 'got offsets for times');
            // merge offsetsForTimes with any non-timestamp based assignments.
            // This merged object will only have assignments with offsets specified, no timestamps.
            return _.flatten([
                assignments.filter(a => !('timestamp' in a) && 'offset' in a),
                offsetsForTimes
            ]);
        })
    }
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
        topic:     kafkaMessage.topic,
        partition: kafkaMessage.partition,
        offset:    kafkaMessage.offset,
        timestamp: kafkaMessage.timestamp || null,
        key:       kafkaMessage.key,
    };

    return kafkaMessage;
}


module.exports = {
    objectFactory:                  objectFactory,
    createKafkaConsumerAsync:       createKafkaConsumerAsync,
    getAvailableTopics:             getAvailableTopics,
    validateAssignments:            validateAssignments,
    topicsToPartitionAssignment:    topicsToPartitionAssignment,
    buildAssignmentsAsync:          buildAssignmentsAsync,
    assignmentsForTimesAsync:       assignmentsForTimesAsync,
    deserializeKafkaMessage:        deserializeKafkaMessage
};
