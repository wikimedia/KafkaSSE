'use strict';

const SSEResponse = require('../lib/SSEResponse');

const _           = require('lodash');
const http        = require('http');
const EventSource = require('eventsource');
const sinon       = require('sinon');
const fetch       = require('node-fetch');
const assert      = require('assert');

const port = 21100;

/**
 * Responds to http requests like: /:n, where n is the number of messages
 * that should be sent back to the SSE client.
 */
class TestSSEServer {
    constructor() {
        this.server = http.createServer();

        this.server.on('request', (req, res) => {
            const messageCountToSend = req.url.replace('/', '');
            sseSendN(req, res, messageCountToSend);
        });
    }

    listen() {
        this.server.listen(port);
    }

    close() {
        this.server.close();
    }
}

function buildMessage(id) {
    return 'data: ' + id;
}

/**
 * Creates a new SSEResponse object and sends n messages.
 */
function sseSendN(req, res, n) {
    let sseResponse = new SSEResponse(res);
    sseResponse.start();

    let id = 0;
    _.times(n, () => {
        const message = buildMessage(id);
        sseResponse.send('message', message, id);
        id++;
    });
}


/**
 * Returns a promise of an HTTP request to an SSE endpoint.
 * Useful for checking http response status of the SSE request.
 */
function httpRequestAsync(port, path) {
    let headers = {
        'Accept': 'text/event-stream',
        'Cache-Control': 'no-cache',
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

describe('SSEResponse', function() {
    const server = new TestSSEServer();

    before(server.listen.bind(server));
    after(server.close.bind(server));

    it('should connect and return 200', (done) => {
        httpRequestAsync(port, 0)
        .then(res => {
            assert.equal(res.status, 200);
            assert.equal(res.headers.get('transfer-encoding'), 'chunked');
            done();
        });
    });

    it('should get 10 messages with Last-Event-ID set propertly', (done) => {
        sseRequest(
            port,
            10,
            spyWithCb(spy => {
                if (spy.callCount >= 10) {
                    // event returned to us is the first arg of the each spied call
                    for (let i = 0; i < 10; i++) {
                        const event = spy.args[i][0];
                        const dataShouldBe = buildMessage(i);
                        const idShouldBe   = i;

                        // Assert that each message has its Last-Event-ID
                        assert.equal(event.lastEventId, idShouldBe);
                        assert.equal(event.data, dataShouldBe);
                    }
                    done();
                }
            })
        );
    });
});
