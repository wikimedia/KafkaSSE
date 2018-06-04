'use strict';

const KafkaSSE     = require('./lib/KafkaSSE');

/**
 * Connects an HTTP request and response to a Kafka Consumer,
 * and sends events to the response in chunked tranfser encoding
 * SSE format.
 *
 * Usage:
 *      const assignments = [
 *          // All partitions in topic1, starting from either latest, or atTimestamp if provided.
 *          'topic1',
 *          // topic2 partition 0, starting at offset 1234
 *          {topic: 'topic2', partition: 0, offset: 1234},
 *          // topic 3 partition 0, starting at the offset at timestamp
 *          {topic: 'topic3', partition: 0, timestamp: 1527861924658 },
 *          // If atTimestamp is given, then topic3 partition 0 starting atTimetsamp, else at latest.
 *          {topic: 'topic3', partition: 0}
 *      ]
 *      const options = {
 *          kafkaConfig: { 'metadata.broker.list':  'localhost:9092' }
 *      }
 *      atTimestamp = 1527858324658 // or undefined
 *
 *      // Stream SSE messages consumed from Kafka assignments to the http response.
 *      KafkaSSE(req, res, assignments, options, atTimestamp)
 *
 *
 * @param {http.ClientRequest}  req
 *
 * @param {http.ServerResponse} res
 *
 * @param {Object|Array} assignments either an array of topic names, a string of comma
 *                delimited topic names, or an array of objects containing
 *                topic, partition, and offset suitable for passing to node-rdkafka
 *                KafkaConsumer assign().  If topic names are given, an assignments object
 *                will be created from them for all partitions in those topics, starting
 *                at latest offset in each.  NOTE: This parameter will be ignored
 *                if req.headers['last-event-id'] is set.  If it is, assignments
 *                will be taken from that header.
 *
 * @param {Object} options      See KafkaSSE options param.
 *
 * @oaram {int}    atTimestamp  Unix milliseconds timestamp.  If provided, any partition
 *                              assignments that don't already have
 *                              offsets will be queryed for the offset atTimestamp.
 */
module.exports = function(req, res, assignments, options, atTimestamp) {
    const kafkaSSE = new KafkaSSE(req, res, options);
    return kafkaSSE.connect(assignments, atTimestamp);
};
