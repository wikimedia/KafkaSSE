'use strict';

const KafkaSSE     = require('./lib/KafkaSSE');

/**
 * Connects an HTTP request and response to a Kafka Consumer,
 * and sends events to the response in chunked tranfser encoding
 * SSE format.
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
 * @param {Object} options  See KafkaSSE options param.
 */
module.exports = function(req, res, assignments, options) {
    const kafkaSSE = new KafkaSSE(req, res, options);
    return kafkaSSE.connect(assignments);
};
