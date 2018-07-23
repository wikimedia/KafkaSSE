'use strict';

const SSEResponse = require('../lib/SSEResponse');

const _           = require('lodash');
const P           = require('bluebird');
const http        = require('http');
const EventSource = require('eventsource');
const fetch       = require('node-fetch');
const assert      = require('assert');

const defaultPort = 21100;


/**
 * Responds to http requests like: /:n, where n is the number of messages
 * that should be sent back to the SSE client.
 */
class TestSSEServer {
    constructor(port, responseOptions, messageBuilder) {
        this.server = http.createServer();
        this.port = port || defaultPort;
        this.messageBuilder = messageBuilder || defaultMessageBuilder;

        this.server.on('request', (req, res) => {
            const messageCount = req.url.replace('/', '');
            sseSendN(res, messageCount, responseOptions, this.messageBuilder);
        });
    }

    listen() {
        return this.server.listen(this.port);
    }

    close() {
        this.server.close();
    }
}

function defaultMessageBuilder(id) {
    return ['message', 'data: ' + id];
}

function objectMessageBuilder(id) {
    return ['message', {'data': id}];
}

function sendN(sse, messageBuilder, n, idx) {
    if (n <= idx) {
        return P.resolve();
    }
    const msg = messageBuilder(idx);
    return sse.send(msg[0], msg[1], idx, 1000)
    .then(() => sendN(sse, messageBuilder, n, idx + 1));
}

/**
 * Creates a new SSEResponse object and sends n messages.
 */
function sseSendN(res, n, options, messageBuilder) {
    let sseResponse = new SSEResponse(res, options);
    sseResponse.start()
    .then(() => sendN(sseResponse, messageBuilder, n, 0))
    .then(() => sseResponse.end());
}

/**
 * Returns a promise of an HTTP request to an SSE endpoint.
 * Useful for checking http response status of the SSE request.
 */
function httpRequestAsync(port, path) {
    let headers = {
        'accept': 'text/event-stream',
        'cache-control': 'no-cache',
    };

    return fetch(
        `http://localhost:${port}/${path}`,
        { headers: headers }
    );
}

/**
 * Uses EventSource to emit events sent from TestSSEServer.
 * msgCb will be called on message, and errCb onerror.
 */
function sseRequest(port, path, msgCb, errCb)  {

    const url = `http://localhost:${port}/${path}`;
    let es = new EventSource(url);

    // If error callback is not given, then just throw the onerror event.
    errCb = errCb || function(e) {
        throw new Error(e);
    }
    es.onerror = (event) => {
        errCb(event);
    };

    if (msgCb) {
        es.onmessage = (event) => {
            // Call msgCb with the event
            msgCb(event);
        }
    }

    return es;
}

class HTTPResponseMock {
    constructor() {
        this.written = [];
        this.finished = false;
    }

    writeHead(httpCode, headers) {
        this.httpCode = httpCode;
        this.headers = headers;
    }

    write(text) {
        this.written.push(text);
        return true;
    }

    headersSent() {
        return this.headers !== undefined;
    }

    finished() {
        return this.finished;
    }

    end(cb) {
        this.finished = true;
        if (cb) {
            cb();
        }
    }
}


describe('SSEResponse', function() {
    const server = new TestSSEServer(defaultPort);

    before(server.listen.bind(server));
    after(server.close.bind(server));

    it('should connect and return 200', (done) => {
        httpRequestAsync(defaultPort, 0)
        .then(res => {
            assert.equal(res.status, 200);
            done();
        });
    });

    it('should return default headers', (done) => {
        httpRequestAsync(defaultPort, 0)
        .then(res => {
            assert.equal(res.headers.get('content-type'), 'text/event-stream');
            assert.equal(res.headers.get('cache-control'), 'no-cache');
            assert.equal(res.headers.get('connection'), 'keep-alive');
            assert.equal(res.headers.get('transfer-encoding'), 'chunked');
            done();
        });
    });

    it('should return custom headers', (done) => {
        const server = new TestSSEServer(1234, {'headers': {
            'content-type': 'modified/content-type',
            'new-header': 'new-value'
        }}).listen();
        httpRequestAsync(1234, 0)
        .then(res => {
            assert.equal(res.headers.get('content-type'), 'modified/content-type');
            assert.equal(res.headers.get('cache-control'), 'no-cache');
            assert.equal(res.headers.get('connection'), 'keep-alive');
            assert.equal(res.headers.get('transfer-encoding'), 'chunked');
            assert.equal(res.headers.get('new-header'), 'new-value');
            server.close();
            done();
        });
    });

    it('should serialize data with default serializer', (done) => {
        const server = new TestSSEServer(1234, {}, objectMessageBuilder).listen();

        sseRequest(
            1234,
            1,
            (event) => {
                const dataShouldBe = JSON.stringify(objectMessageBuilder(0)[1]);
                assert.equal(event.data, dataShouldBe);
                server.close();
                done();
            }
        );
    });

    it('should serialize data with custom serializer', (done) => {
        const server = new TestSSEServer(1234, {
            'serialize': () => { return 'serialized output'; }
        }).listen();

        sseRequest(
            1234,
            1,
            (event) => {
                const dataShouldBe = "serialized output";
                assert.equal(event.data, dataShouldBe);
                server.close();
                done();
            }
        );
    });

    it('should get 10 messages with Last-Event-ID set propertly', (done) => {
        let msgIndex = 0;
        sseRequest(
            defaultPort,
            10,
            (event) => {
                const dataShouldBe = defaultMessageBuilder(msgIndex)[1];
                const idShouldBe   = msgIndex;

                assert.equal(event.lastEventId, idShouldBe);
                assert.equal(event.data, dataShouldBe);

                msgIndex++;
                if (msgIndex === 10) done();
            }
        );
    });

    it('should throw error when event name is missing', () => {
        let rejected = false;
        const res = new HTTPResponseMock();
        const sseResponse = new SSEResponse(res);
        return sseResponse.start()
        .then(() => sseResponse.send(undefined, 'data'))
        .catch((e) => {
            rejected = true;
        }).finally(() => {
            return sseResponse.end().then(() => {
                assert.ok(rejected);
            });
        });
    });

    it('should throw error when event data is missing', () => {
        let rejected = false;
        const res = new HTTPResponseMock();
        const sseResponse = new SSEResponse(res);
        return sseResponse.start()
        .then(() => sseResponse.send('message', undefined))
        .catch((e) => {
            rejected = true;
        }).finally(() => {
            return sseResponse.end().then(() => {
                assert.ok(rejected);
            });
        });
    });

    it('should send event headers', () => {
        const res = new HTTPResponseMock();
        const sseResponse = new SSEResponse(res);
        return sseResponse.start()
        .then(() => sseResponse.send('message', 'data', 1, 1000))
        .then(() => {
            assert.equal(res.written[0], ':ok\n\n');
            assert.equal(res.written[1], 'event: message\n');
            assert.equal(res.written[2], 'retry: 1000\n');
            assert.equal(res.written[3], 'id: 1\n');
            assert.equal(res.written[4], 'data: data\n\n');
        }).finally(() => sseResponse.end());
    });

    it('should handle consecutive line breaks in the data', () => {
        const res = new HTTPResponseMock();
        const sseResponse = new SSEResponse(res);
        return sseResponse.start()
        .then(() => sseResponse.send('message', '1\n2\r\n3\n\n4'))
        .then(() => {
            assert.equal(res.written[0], ':ok\n\n');
            assert.equal(res.written[1], 'event: message\n');
            assert.equal(res.written[2], 'data: 1\n');
            assert.equal(res.written[3], 'data: 2\n');
            assert.equal(res.written[4], 'data: 3\n');
            assert.equal(res.written[5], 'data: 4\n\n');
        }).finally(() => sseResponse.end());
    });
});
