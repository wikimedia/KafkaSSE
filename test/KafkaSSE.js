'use strict';


// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

//  NOTE: these tests require a running kafka broker at localhost:9092

const KafkaSSE       = require('../lib/KafkaSSE');

const _            = require('lodash');
const assert       = require('assert');
const P            = require('bluebird');
const bunyan       = require('bunyan');
const http         = require('http');
const EventSource  = require('eventsource');
const sinon        = require('sinon');
const fetch        = require('node-fetch');


// Kafka host and port used for tests
const kafkaBroker = process.env.KAFKA_BROKERS || 'localhost:9092'

// Topic names used for most tests.  kafkaSSE_test_04 is also used.
const topicNames = [
    'kafkaSSE_test_01',
    'kafkaSSE_test_02',
    'kafkaSSE_test_03'
];

// This topic will be used for some tests, but will not be in the
// a configured list of 'allowed topics'.
const otherTopicName = 'kafkaSSE_test_04'

const serverPort = 6899;
const logLevel   = 'fatal';
// const logLevel   = 'debug';

// a really bad deserializer used for testing custom deserializer function.
const customDeserializedMessage = {
    'i am': 'not a good deserializer',
    _kafka: {
        topic: topicNames[0],
        partition: 0,
        // This offset is bad, but we'll use it for testing the deserializer.
        offset: 12345,
        timestamp: 0,
        key: null,
    }
}

function customDeserializer(kafkaMessage) {
    // deserializer should return a node-rdkafka message,
    // with kafkaMessage.message deserialized from the kafka message value.
    kafkaMessage.message = customDeserializedMessage;
    return kafkaMessage;
}


function customFilterer(kafkaMessage) {
    // only return message with id == 2.
    if (kafkaMessage.message.id == 2) {
        return true;
    }
    else {
        return false;
    }
}

/**
 * KafkaSSE test server.
 * Connect to this with a client on port.
 * Kafka broker must be running at `kafkaBroker`
 * initialized variable value.
 */
class TestKafkaSSEServer {

    constructor(port) {
        this.port = port;
        this.server = http.createServer();

        this.log = bunyan.createLogger({
            name: 'KafkaSSETest',
            level: logLevel,
        });

        // maps 'routes' to KafkaSSE instance options
        const routeOptionsMap = {
            default: {
                logger: this.log,
                kafkaConfig: { 'metadata.broker.list': kafkaBroker }
            },

            // restrictive route will only allow access to specified topics
            restrictive: {
                logger: this.log,
                kafkaConfig: { 'metadata.broker.list': kafkaBroker },
                allowedTopics: topicNames,
            },

            // deserializer route uses a custom deserializer
            deserializer: {
                logger: this.log,
                kafkaConfig: { 'metadata.broker.list': kafkaBroker },
                deserializer: customDeserializer
            },

            // filter route uses a custom filterer
            filter: {
                logger: this.log,
                kafkaConfig: { 'metadata.broker.list': kafkaBroker },
                filterer: customFilterer
            },

            // notopics will always fail, since none of the allowed topics exist.
            notopics: {
                logger: this.log,
                kafkaConfig: { 'metadata.broker.list': kafkaBroker },
                allowedTopics: ['_nope_not_a_topic']
            },
        }

        // Kafka broker should be running at `kafkaBroker`.
        this.server.on('request', (req, res) => {
            // Routes:
            // /default/:topics         - no special options
            // /restrictive/:topics     - only allowed topics
            // /deserializer/:topics    - custom deserializer

            const urlSplit = _.filter(req.url.split('/'), e => e != '');
            const route = urlSplit[0];
            const topics = urlSplit[1].split(',');

            this.log.debug(`Handling ${route} SSE request for topics ${topics}`);

            // get the KafkaSSE options to use for this route
            const options = routeOptionsMap[route];

            const kafkaSSE = new KafkaSSE(req, res, options);
            kafkaSSE.connect(topics);
        });
    }

    listen() {
        this.log.debug('Listening for SSE requests on port ' + this.port);
        this.server.listen(this.port);
    }

    close() {
        this.server.close();
    }
}


assert.topicOffsetsInMessages = (messages, topicOffsets) => {
    topicOffsets.forEach((topicOffset) => {
        let foundIt = messages.find((msg) => {
            return (
                msg._kafka.topic === topicOffset.topic &&
                msg._kafka.partition === topicOffset.partition &&
                msg._kafka.offset === topicOffset.offset
            );
        });
        // assert that messages contained a message
        // consumed from topic at offset.
        assert.ok(foundIt, `message in ${topicOffset.topic} in partition ${topicOffset.partition} at offset ${topicOffset.offset} should be found`);
    });
}

assert.errorNameEqual = (e, errorName) => {
    assert.equal(e.name, errorName, `should error with ${errorName}, got ${e.name} instead`);
}




/**
 * Returns a promise of an HTTP request to an SSE endpoint.
 * Useful for checking http response status of the SSE request.
 */
function httpRequestAsync(port, route, topics, last_event_id) {
    if (_.isArray(topics)) {
        topics = topics.join(',');
    }

    let headers = {
        'Accept': 'text/event-stream',
        'Cache-Control': 'no-cache',
    };
    if (last_event_id) {
        if (!_.isString(last_event_id)) {
            headers['Last-Event-ID'] = JSON.stringify(last_event_id);
        }
        else {
            headers['Last-Event-ID'] = last_event_id
        }
    }

    return fetch(
        `http://localhost:${port}/${route}/${topics}`,
        { headers: headers }
    );
}


/**
 * Creates a new EventSource by connecting to an SSE endpoint.
 * msgCb will be called for each onmessage event, and errCb
 * will be called for each onerror event.  This returns
 * the new EventSource with the onmessage and onerror handlers
 * registered.  msgCb will be given event.data parsed as JSON.
 *
 */
function sseRequest(port, route, topics, last_event_id, msgCb, errCb)  {
    let options = {
        headers: {}
    };

    if (_.isArray(topics)) {
        topics = topics.join(',');
    }

    if (last_event_id) {
        if (!_.isString(last_event_id)) {
            options.headers['Last-Event-ID'] = JSON.stringify(last_event_id);
        }
        else {
            options.headers['Last-Event-ID'] = last_event_id
        }
    }

    const url = `http://localhost:${port}/${route}/${topics}`;
    let es = new EventSource(url, options);

    // If error callback is not given, then just throw the onerror event.
    errCb = errCb || function(e) {
        throw new Error(e);
    }
    es.onerror = (event) => {
        errCb(event);
    };

    if (msgCb) {
        es.onmessage = (event) => {
            // Call msgCb with the event data parsed as JSON.
            msgCb(JSON.parse(event.data));
        }
    }

    return es;
}


/**
 * Returns a Sinon spy that will call fn(spy) whenever
 * the spy is called.
 */
function spyWithCb(fn) {
    let spy = sinon.spy(() => {
        return fn(spy);
    });
    return spy;
}

/**
 * Returns a spy function that once called shouldConsumeOffsets.length times,
 * will assert that kafkaMessages consumed thus far contain the offsets in
 * shouldConsumeOffsets array.  Then calls done();
 */
function assertMessagesConsumedSpy(shouldConsumeOffsets, done) {
    let callCountShouldBe = shouldConsumeOffsets.length;
    return spyWithCb(spy => {
        if (spy.callCount >= callCountShouldBe) {
            // Collect messages from the msgCb that each
            // successful message event has called.
            let kafkaMessages = spy.args.map((args) => args[0]);
            assert.topicOffsetsInMessages(kafkaMessages, shouldConsumeOffsets);
            assert.equal(spy.callCount, callCountShouldBe);
            done();
        }
    });
}



describe('KafkaSSE', function() {
    this.timeout(30000);

    const server = new TestKafkaSSEServer(serverPort);

    before(server.listen.bind(server));
    after(server.close.bind(server));

    it('should connect and return 200', (done) => {
        httpRequestAsync(serverPort, 'default', topicNames[0], undefined)
        .then(res => {
            assert.equal(res.status, 200);
            assert.equal(res.headers.get('transfer-encoding'), 'chunked');
            done();
        })
    });

    it('should connect and return 500 for misconfigured allowedTopics', (done) => {
        httpRequestAsync(serverPort, 'notopics', topicNames[0], undefined)
        .then(res => {
            assert.equal(res.status, 500);
            done();
        })
    });

    it('should connect', (done) => {
        let es = sseRequest(serverPort, 'default', [topicNames[0]]);
        es.onopen = (event) => {
            assert.equal(event.type, 'open');
            done();
        }
    });

    it('should consume 1 message from one topic starting at offset 0', (done) => {
        const assignment = [
            {topic: topicNames[0], partition: 0, offset: 0},
        ];

        const topics = assignment.map(a => a.topic);

        const shouldConsumeOffsets = [
            {topic: topicNames[0], partition: 0, offset: 0},
        ];

        sseRequest(
            serverPort,
            'default',
            topics,
            assignment,
            assertMessagesConsumedSpy(shouldConsumeOffsets, done)
        );
    });

    it('should consume 2 messages from one topic starting at offset 0', (done) => {
        const assignment = [
            {topic: topicNames[1], partition: 0, offset: 0}
        ];

        const topics = assignment.map(a => a.topic);

        const shouldConsumeOffsets = [
            {topic: topicNames[1], partition: 0, offset: 0},
            {topic: topicNames[1], partition: 0, offset: 1},
        ];

        sseRequest(
            serverPort,
            'default',
            topics,
            assignment,
            assertMessagesConsumedSpy(shouldConsumeOffsets, done)
        );
    });

    it('should consume 3 messages from two topics each starting at offset 0', (done) => {
        const assignment = [
            {topic: topicNames[0], partition: 0, offset: 0},
            {topic: topicNames[1], partition: 0, offset: 0},
        ];

        const topics = assignment.map(a => a.topic);

        const shouldConsumeOffsets = [
            {topic: topicNames[0], partition: 0, offset: 0},
            {topic: topicNames[1], partition: 0, offset: 0},
            {topic: topicNames[1], partition: 0, offset: 1},
        ];

        sseRequest(
            serverPort,
            'default',
            topics,
            assignment,
            assertMessagesConsumedSpy(shouldConsumeOffsets, done)
        );
    });

    it('should consume 3 messages from two topics each starting from timestamp 600 seconds ago', (done) => {
        const assignment = [
            {topic: topicNames[0], partition: 0, timestamp: Date.now() - (600*1000)},
            {topic: topicNames[1], partition: 0, timestamp: Date.now() - (600*1000)}
        ];

        const topics = assignment.map(a => a.topic);

        const shouldConsumeOffsets = [
            {topic: topicNames[0], partition: 0, offset: 0},
            {topic: topicNames[1], partition: 0, offset: 0},
            {topic: topicNames[1], partition: 0, offset: 1},
        ];

        sseRequest(
            serverPort,
            'default',
            topics,
            assignment,
            assertMessagesConsumedSpy(shouldConsumeOffsets, done)
        );
    });

    it('should consume 1 message a topic with bad data', (done) => {
        const assignment = [
            {topic: topicNames[2], partition: 0, offset: 0},
        ];

        const topics = assignment.map(a => a.topic);

        const shouldConsumeOffsets = [
            {topic: topicNames[2], partition: 0, offset: 1},
        ];

        sseRequest(
            serverPort,
            'default',
            topics,
            assignment,
            assertMessagesConsumedSpy(shouldConsumeOffsets, done)
        );
    });

    it('should fail subscribe to unavailable topic', (done) => {
        httpRequestAsync(serverPort, 'default', ['_nope_not_a_topic'], undefined)
        .then(res => {
            assert.equal(res.status, 404);
            done();
        });
    });

    it('should fail subscribe to not allowed topic', (done) => {
        httpRequestAsync(serverPort, 'restrictive', [otherTopicName], undefined)
        .then(res => {
            assert.equal(res.status, 404);
            done();
        });
    });

    it('should fail subscribe to unparseable assigments in last-event-id', (done) => {
        const assignment = '"""{nope';
        httpRequestAsync(serverPort, 'default', [topicNames[0]], assignment)
        .then(res => {
            assert.equal(res.status, 400);
            done();
        });
    });

    it('should fail subscribe to invalid assigments in last-event-id', (done) => {
        const assignment = [
            {partition: 0, offset: 0},
        ];
        httpRequestAsync(serverPort, 'default', [topicNames[0]], assignment)
        .then(res => {
            assert.equal(res.status, 400);
            done();
        });
    });

    it('should fail subscribe to unavailable assigments in last-event-id', (done) => {
        const assignment = [
            {topic: '_nope_not_a_topic', partition: 0, offset: 0},
        ];
        httpRequestAsync(serverPort, 'default', [topicNames[0]], assignment)
        .then(res => {
            assert.equal(res.status, 404);
            done();
        });
    });

    it('should fail subscribe to not allowed assigments in last-event-id', (done) => {
        const assignment = [
            {topic: otherTopicName, partition: 0, offset: 0},
        ];

        httpRequestAsync(serverPort, 'restrictive', [topicNames[0]], assignment)
        .then(res => {
            assert.equal(res.status, 404);
            done();
        });
    });

    it('should subscribe and consume with offset reset to latest', (done) => {
        // Since we will produce data to otherTopicName, it is reserved for
        // this test only.
        const topicName = otherTopicName;

        const kafka = require('node-rdkafka');
        var producer = new kafka.Producer({
            'metadata.broker.list': kafkaBroker
        });
        producer = P.promisifyAll(producer, {});

        // Just fail if we encounter any producer errors.
        producer.on('error', (e) => {
            console.log(e);
            throw Error(`Kafka producer threw error: ${e.message}`);
        });

        producer.connectAsync(undefined)
        .then(() => {
            const assignment = [ { topic: topicName, partition: 0, offset: 99999999999 } ];

            const shouldConsumeOffsets = [
                {topic: topicName, partition: 0, offset: 2},
            ];

            // We need to interleave a consume and a produce call.
            // The consume must happen first, so that the non-existent
            // offset can be requested and reset.  Then, a message
            // needs to be produced to the topic.  The consume
            // call should pick up this message.
            sseRequest(
                serverPort,
                'default',
                [topicName],
                assignment,
                assertMessagesConsumedSpy(shouldConsumeOffsets, done)
            );

            // Delay for a bit to make sure the consume request
            //  has time to make it to Kafka.
            return P.delay(3000)
            // Then produce a message;
            .then(() => {
                return producer.produceAsync(
                    topicName,
                    0,
                    new Buffer('{"a": "new message"}')
                );
            });
        });
    });


    // == Test custom message deserializer

    it('should take a custom message deserializer and use it', (done) => {

        const assignment = [
            { topic: topicNames[0], partition: 0, offset: 0 },
        ];

        sseRequest(
            serverPort,
            'deserializer',
            [topicNames[0]],
            assignment,
            spyWithCb(spy => {
                if (spy.callCount >= 1) {
                    // message returned to us is the first arg of the first spied call
                    const message = spy.args[0][0];
                    assert(message['i am'], customDeserializedMessage['i am'], 'message object should be set by custom deserializer');
                    assert(message._kafka.offset, customDeserializedMessage._kafka.offset, 'offset should be set by custom deserializer');
                    done();
                }
            })
        );
    });

    // == Test custom filter function

    it('should take a custom filter function and use it', (done) => {

        const assignment = [
            { topic: topicNames[1], partition: 0, offset: 0 },
        ];

        sseRequest(
            serverPort,
            'filter',
            [topicNames[1]],
            assignment,
            spyWithCb(spy => {
                if (spy.callCount >= 1) {
                    // message returned to us is the first arg of the first spied call
                    const message = spy.args[0][0];
                    assert(message.id, 2, 'should only consume one message with id because of custom filter');
                    done();
                }
            })
        );
    });

});
