'use strict';

const kafka           = require('node-rdkafka');
const P               = require('bluebird');
const bunyan          = require('bunyan');
const _               = require('lodash');
const uuid            = require('uuid');
const EventEmitter    = require('events');

const SSEResponse      = require('./SSEResponse');
const utils            = require('./utils');

const errors                 = require('./error.js');
const ConfigurationError     = errors.ConfigurationError;
const InvalidAssignmentError = errors.InvalidAssignmentError;
const TopicNotAvailableError = errors.TopicNotAvailableError;
const DeserializationError   = errors.DeserializationError;
const FilterError            = errors.FilterError;

/**
 * Represents a Kafka Consumer -> Server Side Events connection.
 *
 * This creates a new Kafka Consumer and passes consumed
 * messages to the connected SSE client.
 *
 * Usage:
 *
 *  let kafkaSse = new KafkaSSE(req, res, options);
 *  kafkaSse.connect(['topicA']);
 */
class KafkaSSE {

    /**
     * @param {http.ClientRequest}  req
     *
     * @param {http.ServerResponse} res
     *
     * @param {Object}   options
     *
     * @param {Object}   options.kafkaConfig: suitable for passing to
     *                   rdkafka.KafkaConsumer constructor.  group.id and
     *                   enable.auto.commit be provided and will be overridden.
     *                   metadata.broker.list defaults to localhost:9092,
     *                   and client.id will also be given a sane default.
     *                   Use the 'default_topic_config' property to configure
     *                   default topic related settings like auto.offset.reset.
     *
     * @param {Object}   options.allowedTopics:  Array of topic names that can be
     *                   subscribed to.  If this is not given, all topics are
     *                   allowed.
     *
     * @param {Object}   options.logger:  bunyan Logger.  A child logger will be
     *                   created from this. If not provided, a new bunyan Logger
     *                   will be created.
     *
     * @param {boolean}  options.disableSSEFormatting: If true, the response body will contain
     *                   only newline delimited messages.  No text/event-stream SSE type
     *                   text formatting will be used.  If this is set, no Last-Event-ID
     *                   will be returned anywhere in the response.
     *
     * @param {function} options.deserializer:  This function takes a single
     *                   node-rdkafka Kafka message and returns the same kafkaMessage, with
     *                   the deserialized kafkaMessage.value as kafkaMessage.message.
     *                   The returned object MUST have topic, partition, and offset fields,
     *                   as these are used to update the latestOffsetsMap which is in turn
     *                   used to populate the SSE id field (last-event-id).  Only
     *                   kafkaMessage.message will be sent to the client as SSE event data.
     *                   If not specified, this will use lib/utils.js/deserializeKafkaMessage.
     *
     * @param {function} options.filterer:  This function takes a deserialized kafkaMessage and
     *                   returns false if the message should be skipped, otherwise true.
     *                   Default: undefined, which means no messages will be skipped.
     *
     * @param {boolean}  options.useTimestampForId:  If set, instead of automatically
     *                   setting the offset in the SSE id field used for Last-Event-ID,
     *                   use the Kafka message's timestamp. This is less precise
     *                   than using offsets, but allows for multi datacenter Kafka
     *                   cluster support. Offsets won't match in different Kafka clusters.
     *                   Default: false.
     *
     * @param {Object}   options.kafkaEventHandlers: Object of
     *                   eventName: function pairs.  Each eventName
     *                   should match an event fired by the node-rdkafka
     *                   KafkaConsumer.  This is useful for installing
     *                   Custom handlers for things like stats and logging
     *                   librdkafka callbacks.
     *
     * @param {function} options.connectErrorHandler: a function that takes
     *                   a single Error parameter.  This function will be called
     *                   if an error is encountered during initialization, before
     *                   the SSE response has started (and any response headers
     *                   have been sent). This allows one to send custom HTTP response headers
     *                   based on the error.  A sane default is provided.
     *                   This function can call res.write(), but must not call
     *                   res.end(), as that is the responsibility of this instance of KafkaSSE.
     *
     * @param {int}      options.idleDelayMs: Number of millseconds to delay between
     *                   consume calls when no new messages are found.  Default: 100

     * @constructor
     */
    constructor(req, res, options) {

        this.req            = req;
        this.res            = res;
        // Use x-request-id if it is set, otherwise create a new uuid.
        this.id             = this.req.headers['x-request-id'] || uuid();

        // Will be set to true for any call to this.disconnect().
        // This will be used to prevent consume loop from
        // attempting to consume from a disconnected kafka client,
        // or sending to a disconnected SSE client.
        this.is_finished    = false;

        // event emitter used to signal the end of processing
        this._eventEmitter = new EventEmitter();

        // Used to keep track of the latest offsets for each topic partition that
        // have been sent to the SSE client.
        this.latestOffsetsMap = {};

        const bunyanConfig = {
            id: this.id,
            serializers: {
                // KafkaSSE Errors know how to serialize themselves well
                // with toJSON.  If an err object has a toJSON method,
                // then use it to serialize the error, otherwise use
                // the standard bunyan error serialize.
                err: (e) => {
                    if (_.isFunction(e.toJSON)) {
                        return e.toJSON();
                    }
                    else {
                        return bunyan.stdSerializers.err(e);
                    }
                }
            }
        };

        this.options = options || {};

        // Set the delay ms between empty kafka consume calls.
        this.idleDelayMs = this.options.idleDelayMs || 100;

        // If we are given a logger, assume it is a bunyan logger
        // and create a child.
        if (this.options.logger) {
            this.log = options.logger.child(bunyanConfig);
        }
        // Else create a new logger, with src logging enabled for dev mode
        else {
            this.log = bunyan.createLogger(
                Object.assign(bunyanConfig, { name: 'KafkaSSE', src: true, level: 'debug' })
            );
        }
        this.log.info(`Creating new KafkaSSE instance ${this.id}.`);

        // Use this to deserialize and augment messages consumed from Kafka.
        this.deserializer = this.options.deserializer || utils.deserializeKafkaMessage;
        this.log.debug(
            { deserializer: this.deserializer.toString() },
            `Deserializing messages with function ${this.deserializer.name}`
        );

        // Check that filterer is a function, and log it.
        if (this.options.filterer) {
            this.filterer = this.options.filterer;

            // Use this to filter messages consumed from Kafka.
            if (!_.isFunction(this.filterer)) {
                throw new ConfigurationError(
                    'filterer option must be a function', this.options.filterer
                );
            }
            this.log.debug(
                { filterer: this.filterer.toString() },
                `Filtering message with function ${this.filterer.name}`
            );
        }

        // Configure connect error handler function.  This function will be called
        // if an error is encountered during connection initialization.
        // Default to writing out an error message and status.
        this.connectErrorHandler = this.options.connectErrorHandler || function(e) {
            if (this.is_finished || this._resFinished()) {
                return;
            }
            try {
                // If Error object has a statusCode property, use it.  Default to 500.
                this.res.statusCode = e.statusCode || 500;
                // TODO if status is 5xx, perhaps we shouldn't return error message to client?
                this.res.statusMessage = e.toString();
                this.res.write(JSON.stringify(e) + '\n');
            } catch(e) {}
        };

        // Default kafkaConfigs to use if not provided in kafkaConfig.
        const defaultKafkaConfig = {
            'metadata.broker.list': 'localhost:9092',
            'client.id': `KafkaSSE-${this.id}`
        };

        // If we won't be responding to any rdkafka events,
        // there is no need to set up the rdkafka event emitter.
        // This is a workaround for a memory leak bug currently
        // in node-rdkafka: https://github.com/Blizzard/node-rdkafka/issues/731
        // If you do set any kafkaEventHandlers, be aware you may encounter a
        // memory leak until this bug is fixed.
        if (!this.options.kafkaEventHandlers) {
            defaultKafkaConfig.event_cb = false;
        }

        // These configs MUST be set for a KafkaSSE KafkaConsumer;
        // they are not overridable.
        // We want to avoid making Kafka manage consumer info for external clients:
        //   A. no offset commits
        //   B. no consumer group management/balancing.
        // A. is achieved simply by setting enable.auto.commit: false.
        // B. is more complicated. Until
        // https://github.com/edenhill/librdkafka/issues/593 is resolved,
        // there is no way to 100% keep Kafka from managing clients.  So,
        // we fake it by using the  name, which will be unique
        // for each socket instance.  Since we use assign() instead of
        // subscribe(), at least we don't have to deal with any rebalance
        // callbacks.
        const mandatoryKafkaConfig = {
            'enable.auto.commit': false,
            'group.id': `KafkaSSE-${this.id}`
        };

        // Merge provided over default configs, and mandatory over all.
        this.kafkaConfig = Object.assign(
            defaultKafkaConfig,
            this.options.kafkaConfig,
            mandatoryKafkaConfig
        );

        // Set up HTTP request and response end handlers.
        // We want to call this.disconnect() in response to any of these
        // events.  We only need to call this.disconnect() the first time
        // one of these happens, so disconnect() will call
        // this.removeHttpEndListners to remove all of these listeners
        // as soon as disconnect() is called.
        const onceReqAbort = () => this.disconnect('HTTP request abort');
        const onceReqAborted = () => this.disconnect('HTTP request aborted');
        const onceReqClose = () => this.disconnect('HTTP request close');
        const onceReqError = (e) => {
            this.log.error(
                {err: e}, 'HTTP request encountered an error, calling KafkaSSE disconnect.'
            );
            this.disconnect('HTTP request error');
        };
        const onceResFinish = () => this.disconnect('HTTP response finish');
        const onceResClose = () => this.disconnect('HTTP response close');
        const onceResError = (e) => {
            this.log.error(
                {err: e}, 'HTTP response encountered an error, calling KafkaSSE disconnect.'
            );
            this.disconnect('HTTP response error');
        };

        this.req.once('abort', onceReqAbort);
        this.req.once('aborted', onceReqAborted);
        this.req.once('close', onceReqClose);
        this.req.once('error', onceReqError);
        this.res.once('finish', onceResFinish);
        this.res.once('close', onceResClose);
        this.res.once('error', onceResError);

        // Will be called theh first time disconnect() is called to keep it
        // from being called multiple times as the connection closes.
        this.removeHttpEndListeners = () => {
            this.log.debug('Removing all HTTP end listeners.');
            if (this.req) {
                this.req.removeListener('abort', onceReqAbort);
                this.req.removeListener('aborted', onceReqAborted);
                this.req.removeListener('close', onceReqClose);
                this.req.removeListener('error', onceReqError);
            }
            if (this.res) {
                this.res.removeListener('finish', onceResFinish);
                this.res.removeListener('close', onceResClose);
                this.res.removeListener('error', onceResError);
            }
        };
    }


    /**
     * Parses assignments, then connects the KafkaConsumer and assigns it.
     * Once that works, the initialization stage is over, and the SSE client
     * is created and the consume loop starts.  Up until _start is called,
     * it is possible to end the request with a sensable HTTP error response.
     * Once _start is called, a 200 response header will be written (via
     * sse.start()), and any further errors must be reported to the
     * client by emitting an error SSE event.
     *
     * Upon encountering any error, this.connectErrorHandler will be called.
     * By default this will send an HTTP error response to the client, and
     * write the JSON serialized error as the response body.
     *
     * @param {Object|Array} assignments either an array of topic names, a string of comma
     *                delimited topic names, or an array of objects containing
     *                topic, partition, and offset suitable for passing to node-rdkafka
     *                KafkaConsumer assign().  If topic names are given, an assignments object
     *                will be created from them for all partitions in those topics, starting
     *                at latest offset in each.  NOTE: This parameter will be ignored
     *                if this.req.headers['last-event-id'] is set.  If it is, assignments
     *                will be taken from that header.
     *
     * @param  {int}  atTimestamp in Unix epoch milliseconds.  If this is provided,
     *                and none of the assignments have offsets, they will
     *                be queried for the offsets at this timestamp, and subscription
     *                will start from there.
     *
     * @return {Promise} This promise will only resolve if the client closes the request.
     *                   It is not expected to be rejected, as errors are caught and logged
     *                   and sent to the client.
     */
    connect(assignments, atTimestamp) {
        // Initialization stage.
        // NOTE: we do not return this Promise because we want the processing to happen outside
        // the main promise chain. Instead, a promise is returned that awaits for the disconnect()
        // method to complete, after which point it yields.
        this._init(assignments, atTimestamp)
        .then(() => {
            // Consume Loop SSE stage.
            // If we get here, then we know that initialization succeeded.
            return this._start()

            // Consume loop SSE error handling:
            // If anything bad happens during operation, emit an SSE error.
            .catch((e) => {
                // Log and make the error JSON serializable.
                e = this._error(e);

                // Emit an error event to the connected SSE client.
                if (this.sse && !this._resFinished()) {
                    return this.sse.send('error', e, _.values(this.latestOffsetsMap))
                    // if an error occurs here ignore it since the only thing
                    // that can go wrong here is not being able to deliver the
                    // error message to the client
                    .catch((sseSendError) => {
                        this.log.error(
                            {err: sseSendError},
                            'Caught error while attempting to send SSE error event'
                        );
                    });
                }
            });
        })

        // Initialization stage error handling.
        // If anything bad happens during initialization,
        // call the configured connectErrorHandler.  The error handler should
        // write HTTP response headers and body, but should not call res.end().
        // That should be done by this.disconnect() in the .finally block below().
        .catch((e) => {
            // Log and make the error JSON serializable.
            e = this._error(e);
            this.connectErrorHandler(e);
        })
        .finally(() => {
            // Just in case we get here for a reason other than the HTTP request closing
            // or error handling, make sure we disconnect everything properly,
            // especially the KafkaConsumer.
            if (!this.is_finished) {
                return this.disconnect('Finally finished consume loop and error handling');
            }
        });

        // connect() will resolve after the KafkaSSE `done` event is fired by disconnect().
        return new P((resolve, reject) => {
            this._eventEmitter.on('done', () => {
                this.log.info('KafkaSSE connection done.');
                resolve();
            });
        });
    }


    /**
     * Creates the Kafka Consumer and assigns it to start consuming at assignments.
     *
     * @param {Object|Array} assignments either an array of topic names, a string of comma
     *                delimited topic names, or an array of objects containing
     *                topic, partition, and offset suitable for passing to node-rdkafka
     *                KafkaConsumer assign().  If topic names are given, an assignments object
     *                will be created from them for all partitions in those topics, starting
     *                at latest offset in each.
     *
     * @return {Promise<Object>} assigned assignments
     */
    _init(assignments, atTimestamp) {

        // Parse assignments, using last-event-id request header if it is set.
        return new P((resolve, reject) => {
            // KafkaSSE instances are one time use only.
            if (this.is_finished || this._resFinished()) {
                throw new ConfigurationError('Cannot re-use a KafkaSSE instance.', {
                    statusCode: 500
                });
            }

            if ('last-event-id' in this.req.headers) {
                try {
                    assignments = JSON.parse(this.req.headers['last-event-id']);
                }
                catch (e) {
                    // re raise as InvalidAssignmentError.
                    throw new InvalidAssignmentError(e, {
                        last_event_id: this.req.headers['last-event-id'],
                        // Recommended http status response for this error
                        statusCode: 400
                    });
                }
            }
            // else assignments will be used as passed in.

            // Convert to an Array if we were given a string.
            if (_.isString(assignments)) {
                assignments = assignments.split(',');
            }

            // Validate assignments.
            try {
                utils.validateAssignments(assignments);
            }
            catch (e) {
                // re raise as InvalidAssignmentError.
                throw new InvalidAssignmentError(e, {
                    // Recommended http status response for this error
                    statusCode: 400
                });
            }

            resolve(assignments);
        })

        // Create and connect a new KafkaConsumer instance
        .then(() => utils.createKafkaConsumerAsync(this.kafkaConfig))

        // Save the consumer and register any configured rdkafka event handlers
        .then(consumer => {
            // Save our consumer.
            this.kafkaConsumer = consumer;

            // TODO: tests for this:
            if (this.options.kafkaEventHandlers) {
                Object.keys(this.options.kafkaEventHandlers).forEach((event) => {
                    this.log.debug(
                        `Registering Kafka event ${event} to be handled by ` +
                        `function ${this.options.kafkaEventHandlers[event].name}`
                    );
                    this.kafkaConsumer.on(event, this.options.kafkaEventHandlers[event]);
                });
            }

            return this.kafkaConsumer;
        })

        // Save intersection of allowedTopics and existent topics as this.availableTopics.
        // We need to connect the KafkaConsumer above before we do this
        // as the KafkaConsumer tells us what topics exist in Kafka.
        .then(() => {
            this.availableTopics = utils.getAvailableTopics(
                this.kafkaConsumer._metadata.topics,
                this.options.allowedTopics
            );
            // Throw Error if there are no available topics.  This
            // will fail initialization and disconnect the client.
            if (this.availableTopics.length === 0) {
                throw new ConfigurationError(
                    'No topics available for consumption. ' +
                    'This likely means that the configured allowedTopics ' +
                    'do not currently exist.',
                    {
                        allowedTopics: this.options.allowedTopics,
                        // Recommended http status response for this error
                        statusCode: 500
                    }
                );
            }
        })

        // Check that assignments are allowed and build final assignments
        // object from string topic names and/or timestamp offests queried and replaced
        // with real offsets.
        .then(() => {
            // Get list of topics from assignments.  If entry is a string,
            // assume it is the topic.  If it is an object, then extract
            // it from the topic property.
            this.topics = _.uniq(assignments.map(a => _.isString(a) ? a : a.topic));
            // Validate that all requested topics are available for consumption.
            // This will throw an exception if they are not.
            this._checkTopicsAvailable(this.topics);

            // Convert provided assignments to something suitable to pass to
            // kafkaConsumer.assign.  For each entry in the assignments array:
            //
            // - string topic entries will be converted to partition assignment objects
            //   starting from either atTimestamp, or at -1 (latest).
            //
            // - object entries with offset already set will be left as is
            //
            // - object entries without offset but with timestamp will start
            //   from offset for the given timestamp
            //
            // - object entries without offset or timestamp will start from atTimestamp,
            //   or at -1 (latest).
            this.log.debug(
                {assignments: assignments, atTimestamp: atTimestamp},
                'Bulding assignments from passed in assignments'
            );
            return utils.buildAssignmentsAsync(this.kafkaConsumer, assignments, atTimestamp);
        })
        // Save the assignments and initialize the latestOffsetsMap from assignments,
        .then((assignments) => {
            this.assignments = assignments;
            this.log.debug({assignments: this.assignments}, 'Final resolved Kafka assignments');

            // The client will be sent latest offsets as assignments
            // as the SSE id for each message.
            this.assignments.forEach((a) => {
                this.latestOffsetsMap[`${a.topic}/${a.partition}`] = a;
            });
        })
        // Assign the KafkaConsumer
        .then(() => {
            this.log.info(
                { assignments: this.assignments },
                `Subscribing to (some) topic partitions in topics ${this.topics.join(',')}.`
            );

            return P.resolve(this.kafkaConsumer.assign(this.assignments))
            .thenReturn(this.assignments);
        });
    }


    /**
     * Throws TopicNotAvailableError if any topic in topics Array is not
     * in the list of available topics.
     *
     * @param {Array} topics
     * @throws TopicNotAvailableError if any of the topics are not available.
     */
    _checkTopicsAvailable(topics) {
        // Find any invalid/unavailable topics and throw an Error reporting them.
        const unavailableTopics = topics.filter(topic => !_.includes(this.availableTopics, topic));
        if (unavailableTopics.length > 0) {
            throw new TopicNotAvailableError(
                `Topics ${unavailableTopics.join(', ')} are not available for consumption.`,
                {
                    availableTopics: this.availableTopics,
                    // Recommended http status response for this error
                    statusCode: 404
                }
            );
        }
    }


    _start() {
        // Start the consume -> sse send loop.
        this.log.info('Initializing KafkaSSE connection and starting consume loop.');

        const responseHeaders = {};
        let disableSSEFormatting = false;

        // If user wants application/json, then disableSSEFormatting.
        // Else just use text/event-stream SSE formatting.
        if (_.has(this.req.headers, 'accept')) {
            if (this.req.headers['accept'].startsWith('application/json')) {
                responseHeaders['content-type'] = 'application/json; charset=utf-8';
                disableSSEFormatting = true;
            } else {
                responseHeaders['content-type'] = 'text/event-stream; charset=utf-8';
            }
        }

        // Initialize the SSEResponse and start sending
        // the response in chunked transfer encoding.
        this.sse = new SSEResponse(this.res, {
            headers: responseHeaders,
            disableSSEFormatting,
            log: this.log
        });
        return this.sse.start()
        // Loop 'forever'.
        .then(() => this._loop());
    }


    /**
     * Consume, send a message, and then consume again.
     * This will loop forever, until disconnect or error.
     */
    _loop(startTime) {
        startTime = startTime || Date.now();
        // Consume, send the message, and then consume again
        // This will loop forever, until disconnect or error.
        return this._consume()

        .then((kafkaMessage) => {
            // If the request is finished (something called this.disconnect()),
            // then exit the consume loop now by returning a resolved promise.
            if (this.is_finished) {
                this.log.debug('KafkaSSE connection finished. Returning from consume loop.');
                return P.resolve();
            }
            if (!kafkaMessage || !kafkaMessage.message || !kafkaMessage.topic) {
                this.log.debug({ message: 'Wrongly formatted message', event: kafkaMessage });
                return P.delay(this.idleDelayMs).then(() => this._loop());
            }
            // Else we got a kafkaMessage.  Update the latestOffsetsMap
            // and send the kafkaMessage.message to the sse client.
            this._updateLatestOffsetsMap(kafkaMessage);

            // Send the message event and the updated offsets to the sse client.
            return this.sse.send(
                'message',
                kafkaMessage.message,
                _.values(this.latestOffsetsMap)
            ).then(() => {
                // we need to give the V8 loop time to process I/O callbacks
                // every once in a while, so skip a millisecond of processing
                // at least every 1.5 seconds
                if (Date.now() - startTime > 1500) {
                    return new P((resolve, reject) => {
                        setTimeout(
                            () => resolve(Date.now()),
                            1
                        );
                    });
                }
                return P.resolve(startTime);
            }).then((t) => this._loop(t));
        });
    }


    /**
     * Given a kafkaMessage consumed from node-rdkafka KafkaConsumer,
     * This will update the this.latestOffsetsMap with the kafkaMessage's
     * topic, partition, and offset|timestamp + 1.
     *
     * NOTE: + 1 is necessary for Last-Event-ID for EventSource auto-resuming on reconnect.
     * Without the + 1, the previously consumed message will be reconsumed.
     *
     * @param {KafkaMessage} kafkaMessage
     */
    _updateLatestOffsetsMap(kafkaMessage) {
        if (!('latestOffsetsMap' in this)) {
            this.latestOffsetsMap = {};
        }

        // Add this message's id to latestOffsetsMap object
        this.latestOffsetsMap[`${kafkaMessage.topic}/${kafkaMessage.partition}`] = {
            topic:     kafkaMessage.topic,
            partition: kafkaMessage.partition,
        };

        // Set either timestamp or offset, depending on the configuration of useTimestampForId
        if (this.options.useTimestampForId) {
            this.latestOffsetsMap[`${kafkaMessage.topic}/${kafkaMessage.partition}`].timestamp = kafkaMessage.timestamp + 1;
        }
        else {
            this.latestOffsetsMap[`${kafkaMessage.topic}/${kafkaMessage.partition}`].offset = kafkaMessage.offset + 1;
        }

        return this.latestOffsetsMap;
    }


    /**
     * Consumes messages from Kafka until we find one that parses and matches
     * via the matcher function, and then returns a promise that includes
     * the matched and deserialized message.
     *
     * @return {Promise<Object>}
     */
    _consume() {
        // If we have finished (by calling this.disconnect),
        // don't try to consume anything.
        if (this.is_finished) {
            this.log.debug('KafkaSSE connection finished. Not attempting consume.');
            return P.resolve();
        }

        // Consume a message from Kafka
        return this.kafkaConsumer.consumeAsync(1)
        .then((kafkaMessages) => {
            if (!kafkaMessages || !kafkaMessages.length) {
                // No messages to receive. Delay a bit.
                return P.delay(this.idleDelayMs);
            }

            // Deserialize the consumed message if deserializer function is provided.
            return this._deserialize(kafkaMessages[0])
            // Filter the deserialized message if a filter function is provided.
            .then((msg) => this._filter(msg))
        })

        // Catch Kafka errors, log and re-throw real errors,
        // ignore harmless ones.
        .catch({ origin: 'kafka' }, (e) => {
            // Ignore innoculous Kafka errors.
            switch (e.code) {
                case kafka.CODES.ERRORS.ERR__PARTITION_EOF:
                case kafka.CODES.ERRORS.ERR__TIMED_OUT:
                    this.log.trace(
                        { err: e },
                        'Encountered innoculous Kafka error: ' +
                        `'${e.message} (${e.code}). Delaying 100 ms before continuing.`
                    );
                    // Delay a small amount after innoculous errors to keep
                    // the consume loop from being so busy when there are no
                    // new messages to consume.
                    return P.delay(this.idleDelayMs);
                default:
                    this.log.error(
                        { err: e },
                        'Caught Kafka error while attempting to consume a message.'
                    );
                    throw e;
            }
        })
        // Log and ignore DeserializationError.  We don't want to fail the
        // client if the data in a topic is bad.
        .catch(DeserializationError, (e) => {
            this._error(e, 'warn');
            return P.resolve(false);
        })
        .catch((e) => {
            this._error(e, 'warn');
            return P.resolve(false);
        })
        // Any unexpected error will be thrown to the client and not caught here.

        // If we found a message, return it, else keep looking.
        .then((kafkaMessage) => {
            if (kafkaMessage) {
                this.log.trace({ message: kafkaMessage.message }, 'Consumed message.');
                return kafkaMessage;
            }
            else {
                this.log.trace('Have not yet found a message while consuming, trying again.');
                return this._consume();
            }
        });
    }


    /**
     * Returns a Promise of an Object deserialized from kafkaMessage.
     * This calls the this.deserializer function configured using
     * options.deserializer in the constructor.
     * If this.deserializer throws any error, the error will be
     * wrapped as a DeserializationError and thrown up to the caller.
     *
     * @param  {Object}          kafkaMessage from a KafkaConsumer.consume call.
     * @return {Promise{Object}} derialized and possible augmented kafkaMessage.
     */
    _deserialize(kafkaMessage) {
        return new P((resolve, reject) => {
            if ('deserializer' in this) {
                kafkaMessage = this.deserializer(kafkaMessage);
            }

            resolve(kafkaMessage);
        })
        // Catch any error thrown by this.deserializer and
        // wrap it as a DeserializationError.
        .catch((e) => {
            throw new DeserializationError(
                'Failed deserializing and building message from Kafka: ' + e.toString(),
                { kafkaMessage: kafkaMessage, originalError: e }
            );
        });
    }

    /**
     * Returns a Promise of a deserialized kafkaMessage,
     * or false if this kafkaMessage should be skipped.
     * This calls the this.filterer function configured using
     * options.filterer in the constructor.
     * If this.filterer throws any error, the error will be
     * wrapped as a FilterError and thrown up to the caller.
     *
     * @param  {Object}          Deserialized kafkaMessage
     * @return {Promise{Object}} false if should skip, or kafkaMessage if should keep
     */
    _filter(kafkaMessage) {
        return new P((resolve, reject) => {
            // If filterer is not set, or if kafkaMessage passes filterer,
            // resolve kafkaMesssage;
            if (!this.filterer || this.filterer(kafkaMessage)) {
                resolve(kafkaMessage);
            }
            // Else filterer returned false, so resolve false to
            // indicate this message should be skipped.
            else {
                this.log.trace(
                    { kafkaMessage: kafkaMessage },
                    'Filtering out and skipping kafkaMessage.'
                );
                resolve(false);
            }
        })
        // Catch any error thrown by this.filterer and
        // wrap it as a FilterError.  This error will be sent to the
        // client as an SSE error event, and then the client will
        // be disconnected.
        .catch((e) => {
            throw new FilterError(
                'Failed filtering message: ' + e.toString(),
                { kafkaMessage: kafkaMessage, originalError: e }
            );
        });
    }

    /**
     * Logs error, and returns error with deleted stack, suitable for
     * returning to clients.
     *
     * @param  {Error}  e
     * @param  {string} level log level to use
     * @return {Object} serialized error without stack.
     */
    _error(e, level) {
        level = level || 'error';
        level = this.log[level] ? level : 'error';
        if (typeof e === 'string') {
            e = new Error(e);
        }
        this.log[level]({ err: e });

        // Delete the stack trace for the
        // error that will be sent to client.
        delete e.stack;

        return e;
    }

    /**
     * Checks if the HTTP response is finished.
     * Returns true if if this.res is undefined, or if the this.res.finished,
     * or if res.connection.destroyed.
     *
     * @return {boolean}
     */
    _resFinished() {
        const res = this.res;
        if (!res || res.finished || (res.connection && res.connection.destroyed)) {
            return true;
        }
        return false;
    }


    /**
     * Disconnects the KafkaConsumer and closes the sse client and/or http response.
     * If disconnect() has already been called, this does nothing.
     *
     * @param {string} reason Reason disconnect was called, used for logging.
     * @return {Promise} Resolved if disconnect was successful, rejected if errored.
     */
    disconnect(reason) {
        reason = reason || 'unknown';
        // If disconnect has already been called once, do nothing.
        // This shouldn't really happen, as the HTTP end listeners will be removed.
        if (this.is_finished) {
            this.log.debug(`KafkaSSE disconnect() has already been called. Doing nothing in response to: ${reason}`);
            return P.resolve();
        }
        this.is_finished = true;

        this.log.info(`KafkaSSE disconnecting due to: ${reason}`);

        // Remove other HTTP disconnect handlers to prevent disconnect from being fired multiple times.
        this.removeHttpEndListeners();

        const disconnectHttpPromise = P.resolve().then(() => {
            // 3 cases:
            // - Usually we are disconnecting an active SSEResponse, so just
            //   end the HTTP response via this.sse.end.
            // - If no SSEResponse is active, just end this.res.
            // - Else this.res is finished but we still have a reference to it
            //   so just delete this.res.
            if (this.sse) {
                // If we still have SSE, then end the SSEResponse.
                // SSEResponse will handle ending thee HTTP Response itself.
                const sse = this.sse;
                delete this.sse;
                delete this.res;

                return sse.end()
                .then(() => this.log.debug('KafkaSSE disconnect: Ended SSE (HTTP) response.'));
            } else if (!this._resFinished()) {
                // Else if for disconnect was called and we don't have an SSEResponse,
                // (likely because the SSEResponse wasn't ever started), then just
                // end the HTTP response here.
                return new P((resolve, reject) => {
                    const res = this.res;
                    if (this._resFinished()) {
                        delete this.res;
                        return resolve();
                    }

                    delete this.res;
                    res.once('error', reject);
                    try {
                        res.end();
                        resolve();
                    } catch(e) {
                        reject(e);
                    }
                })
                .then(() => this.log.debug('KafkaSSE disconnect: Ended HTTP response.'));
            } else if (this.res) {
                // Else the HTTP Response has already been ended,
                // so just delete our reference to it.
                delete this.res;
                this.log.debug('KafkaSSE disconnect: Deleted HTTP response.');
                return P.resolve();
            }
        });

        const disconnectKafkaPromise = P.resolve().then(() => {
            if (!this.kafkaConsumer) {
                this.log.debug("KafkaSSE disconnect: Kafka consumer already deleted, doing nothing.");
                return P.resolve();
            }
            const kafkaConsumer = this.kafkaConsumer;
            delete this.kafkaConsumer;

            return kafkaConsumer.disconnectAsync()
            .then(() => this.log.debug("KafkaSSE disconnect: Disconnected the Kafka consumer."));
        });

        // Emit 'done' when both HTTP response and Kafka are disconnected.
        return P.all([disconnectHttpPromise, disconnectKafkaPromise])
        .catch((e) => {
            this.log.error({err: e}, 'KafkaSSE disconnect: encountered error');
        })
        .finally(() => {
            this.log.debug('KafkaSSE disconnect: finished.')
            return this._eventEmitter.emit('done')
        });

    }
}

module.exports = KafkaSSE;
