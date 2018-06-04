#!/usr/bin/env node
'use strict';


const http =  require('http');
const kafkaSseHandler = require('./index');

const port = 6927;

const kafkaBroker = 'localhost:9092'

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
            const splitUrl = req.url.replace('/', '').split("?timestamp=");
            const topics = splitUrl[0].split(',');
            console.log(`Handling SSE request for topics ${topics}`);
            const options = {
                kafkaConfig: { 'metadata.broker.list':  kafkaBroker }
            }

            let atTimestamp = splitUrl.length > 1 ? Number(splitUrl[1]) : undefined;

            // const atTimestamp = 'timestamp' in req.params ? Number(req.params.timestamp) : undefined;
            kafkaSseHandler(req, res, topics, options, atTimestamp);
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
