#!/usr/bin/env node
'use strict';

/**
 * Example nodejs EventSource KafkaSSE client.
 *
 * Usage:
 *   ./client.js topic1,topic2 [port]
 * (port defauts to 6927)
 */

const EventSource = require('eventsource');

const topics = process.argv[2];
const port = process.argv[3] || 6927;

const url = `http://localhost:${port}/${topics}`;
console.log(`Connecting to Kafka SSE server at ${url}`);
let eventSource = new EventSource(url);

eventSource.onopen = function(event) {
    console.log('--- Opened SSE connection.');
};

eventSource.onerror = function(event) {
    console.log('--- Got SSE error', event);
};

eventSource.onmessage = function(event) {
    // event.data will be a JSON string containing the message event.
    console.log(JSON.parse(event.data));
};

