#!/usr/bin/env node
'use strict';

/**
 * Example nodejs EventSource KafkaSSE client.
 *
 * Usage:
 *   ./client.js topic1,topic2 [timestamp] [port]
 * (port defauts to 6927)
 */

const EventSource = require('eventsource');

const topics = process.argv[2].split(',');
const timestamp = process.argv[3] || undefined;
const port = process.argv[4] || 6927;


let url = `http://localhost:${port}/${topics}`;
if (timestamp) {
    url += `?timestamp=${timestamp}`;
}
console.log(`Connecting to Kafka SSE server at ${url}`);



// If used, consume from 'hi' starting at offset.
const simpleAssignments = [
    {topic: 'hi', partition: 0, offset: 22060}
];

// Or consume from topic starting 1 hour ago.
const timestampAssignments = topics.map((t) => {
    return {topic: t, partition: 0, timestamp: Date.now() - (3600*1000)};
});


let options = {
    headers: {}
};


// if not provided timestamp on CLI, use test timestampAssignments.
if (!timestamp) {
    options.headers['Last-Event-ID'] = JSON.stringify(timestampAssignments);
}


// options.headers['Last-Event-ID'] = JSON.stringify(simpleAssignments);

let eventSource = new EventSource(url, options);

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
