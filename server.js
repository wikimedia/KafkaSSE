#!/usr/bin/env node
'use strict';


const http =  require('http');
const kafkaSseHandler = require('./index');

const port = 6927;

/**
 * Kasse test server.
 * Connect to this endpoint at localhost:${port}/:topics.
 * Kafka broker must be running at localhost:9092.
 * NOTE: This is just an example server.  You should
 * probably instantiate your own http server instnaces
 * and handle http requests with KafkaSSE instances in your own app.
 */
class KafkaSSEServer {

    constructor() {
        this.server = http.createServer();
        this.server.on('request', (req, res) => {
            const topics = req.url.replace('/', '').split(',');
            console.log(`Handling SSE request for topics ${topics}`);
            kafkaSseHandler(req, res, topics);
        });
    }

    listen() {
        this.server.listen(port);
        console.log(`Listening for HTTP SSE connections at on port ${port}`);
    }
}

if (require.main === module) {
    new KafkaSSEServer().listen();
}

module.exports = KafkaSSEServer;
