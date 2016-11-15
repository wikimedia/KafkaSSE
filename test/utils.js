'use strict';


// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

const assert = require('assert');

const utils = require('../lib/utils.js');

const topicsInfo = [
    { name: 'test0', partitions: [
        { id: 0, leader: 0, replicas: [ 0 ], isrs: [0] }
    ] },
    { name: 'test1', partitions: [
        { id: 0, leader: 0, replicas: [ 0 ], isrs: [0] },
        { id: 1, leader: 0, replicas: [ 0 ], isrs: [0] }
    ] },
];


describe('objectFactory', () => {
    const o = {
        'a': 'b',
        'o2': {
            'e': 1,
            'r': 'abbbbc',
        },
        'array': ['a', 'b', 'c']
    };

    it('should return same object if given object', () => {
        assert.equal(utils.objectFactory(o), o);
    });

    it('should return object from JSON string', () => {
        assert.deepEqual(utils.objectFactory(JSON.stringify(o)), o);
    });

    it('should return object from JSON Buffer', () => {
        let buffer = new Buffer(JSON.stringify(o));
        assert.deepEqual(utils.objectFactory(buffer), o);
    });

    it('should fail with wrong type', () => {
        assert.throws(utils.objectFactory.bind(undefined, 12345));
    });
});


describe('getAvailableTopics', () => {
    it('should return existent topics if allowedTopics is not specified', () => {
        let availableTopics = utils.getAvailableTopics(topicsInfo);
        assert.deepEqual(
            availableTopics,
            ['test0', 'test1']
        );
    });

    it('should return intersection of allowedTopics and existent topics', () => {
        let allowedTopics = ['test1'];
        let availableTopics = utils.getAvailableTopics(topicsInfo, allowedTopics);
        assert.deepEqual(
            availableTopics,
            allowedTopics
        );
    });
});


describe('validateAssignments', () => {
    it('should validate an array of topic names', () => {
        assert.ok(utils.validateAssignments(['a', 'b', 'c']));
    });

    it('should validate an array of assignment objects', () => {
        let assignments = [
            { topic: 'test0', partition: 0, offset: 123 },
            { topic: 'test1', partition: 0, offset: 456 },
            { topic: 'test1', partition: 1, offset: 234 }
        ]
        assert.ok(utils.validateAssignments(assignments));
    });

    it('should fail validation of a non array', () => {
        assert.throws(utils.validateAssignments.bind(undefined, 'nope'));
    });

    it('should fail validation of an empty array', () => {
        assert.throws(utils.validateAssignments.bind(undefined, []));
    });

    it('should fail validation of topic names with bad elements', () => {
        assert.throws(utils.validateAssignments.bind(undefined, ['test0', 1234]));
    });

    it('should fail validation of assignment objects with bad elements', () => {
        assert.throws(utils.validateAssignments.bind(undefined,
            [{ topic: 'test1', partition: 1, offset: 234 }, 1234]
        ));
    });

    it('should fail validation of assignment objects missing a property', () => {
        assert.throws(utils.validateAssignments.bind(undefined,
            //  this is missing 'offset'
            [{ topic: 'test1', partition: 1, } ]
        ));
    });

    it('should fail validation of assignment objects with bad properties', () => {
        assert.throws(utils.validateAssignments.bind(undefined,
            [{ topic: 1234, partition: 1, offset: 234 } ]
        ));
        assert.throws(utils.validateAssignments.bind(undefined,
            [{ topic: 'test1', partition: 'hi', offset: 234 } ]
        ));
        assert.throws(utils.validateAssignments.bind(undefined,
            [{ topic: 'test1', partition: 1, offset: ['bad', 'offset'] } ]
        ));
    });
});


describe('buildAssignments', () => {
    it('should return empty array if topic does not exist', () => {
        let assignments = utils.buildAssignments(topicsInfo, ['does-not-exist']);
        assert.deepEqual(
            assignments,
            []
        );
    });

    it('should return assignments for a single partition topic', () => {
        let assignments = utils.buildAssignments(topicsInfo, ['test0']);
        assert.deepEqual(
            assignments,
            [ { topic: 'test0', partition: 0, offset: -1 } ]
        );
    });

    it('should return assignments for a multiple partition topic', () => {
        let assignments = utils.buildAssignments(topicsInfo, ['test1']);
        assert.deepEqual(
            assignments,
            [
                { topic: 'test1', partition: 0, offset: -1 },
                { topic: 'test1', partition: 1, offset: -1 }
            ]
        );
    });

    it('should return assignments for a multiple topics', () => {
        let assignments = utils.buildAssignments(topicsInfo, ['test0', 'test1']);
        assert.deepEqual(
            assignments,
            [
                { topic: 'test0', partition: 0, offset: -1 },
                { topic: 'test1', partition: 0, offset: -1 },
                { topic: 'test1', partition: 1, offset: -1 }
            ]
        );
    });
});

describe('deserializeKafkaMessage', () => {
    it('should return an augmented message from a Kafka message', function() {
        let kafkaMessage = {
            value: new Buffer('{ "first_name": "Dorkus", "last_name": "Berry" }'),
            topic: 'test',
            partition: 1,
            offset: 123,
            key: 'myKey',
        };

        let msg = utils.deserializeKafkaMessage(kafkaMessage);
        assert.equal(msg.message.first_name, "Dorkus");
        assert.equal(msg.message._kafka.topic, kafkaMessage.topic, 'built message should have topic');
        assert.equal(msg.message._kafka.partition, kafkaMessage.partition, 'built message should have partition');
        assert.equal(msg.message._kafka.offset, kafkaMessage.offset, 'built message should have offset');
        assert.equal(msg.message._kafka.key, kafkaMessage.key, 'built message should have key');
    });
});



//  TODO: other utils.js unit tests
