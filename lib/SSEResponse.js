'use strict';

const _ = require('lodash');
const P = require('bluebird');

/**
 * Wraps an HTTP response to ease sending SSE formatted events
 * via text/event-stream and chunked transfer encoding.
 *
 * Usage:
 *  sse = new SSEResponse(req, res);
 *  sse.start();
 *  sse.send('message', {'my': 'first message'},  1001);
 *  sse.send('message', {'my': 'second message'}, 1002);
 *  // or with a complex Last-Event-ID
 *  sse.send('message', {'my': 'third message'}, [{'partition': 0, 'offset': 1003}]);
 *
 * This class was originally stolen and then modified from
 * https://github.com/einaros/sse.js/blob/master/lib/sseclient.js.
 */
class SSEResponse {
   /**
    * @param {http.ServerResponse} res
    * @param {Object} options
    * @param {Object} options.headers: Extra headers to add to the SSE response.
    *                 The defaults are: {
    *                     'Content-Type': 'text/event-stream',
    *                     'Cache-Control': 'no-cache',
    *                     'Connection': 'keep-alive'
    *                 }
    *                 Anything passed in options.headers will be merged over the defaults.
    *
    * @param {function} options.serialize: Function to serialize data and id.  Default: toJSON
    *
    * @constructor
    */
    constructor(res, options) {
        this.res = res;
        this.options = options || {};
        this.serialize = this.options.serialize || toJSON;
        this.headers = {
            'content-type': 'text/event-stream',
            'cache-control': 'no-cache',
            'connection': 'keep-alive'
        };

        // Normalize optional extra response headers to lowercase
        // (should be case insensitive) and merge with default ones.
        _.each(this.options.headers, (value, field) => {
            this.headers[field.toLowerCase()] = value;
        });
    }


    /**
     * Starts sending the SSE response body.
     */
    start() {
        this.res.writeHead(200, this.headers);
        return this._write(':ok\n\n');
    }


    /**
     * Sends a single SSE event.  id and retry are optional, event and data are mandatory.
     *
     * @param string          event: event name, e.g. 'message'
     * @param {Object|string} data:  event data. this.serialize(data) will be called before sending.
     * @param {Object|string} id:    event ID. this.serialize(id) will be called before sending.
     * @param {integer}       retry: Retry milliseconds for EventSource client.
     *                               This sets the reconnection time on disconnect for the
     *                               connected EventSource client. (optional)
     */
    send(event, data, id, retry) {
        // the raw lines to be sent to the client
        let raw = [];
        let p = P.resolve();

        if (!event) {
            return P.reject('Cannot send SSE event.  Must provide event name.');
        }
        if (!data) {
            return P.reject('Cannot send SSE event.  Must provide event data.');
        }

        // Start the text/event-stream response if it hasn't already started.
        if (!this.res.headersSent) {
            p = this.start();
        }

        // Serialize the data and the id, since those may be complex
        // objects interpreted by EventSource clients.
        // (event and retry are not objects.)
        // Also, replace any consecutive occurences of \r or \n with a single \n,
        // so the data doesn't accidentally end the event prematurely.
        data = this.serialize(data).replace(/[\r\n]+/g, '\n');
        id = this.serialize(id);

        // Set the event headers (event name, retry, id)
        if (event) {
            raw.push(`event: ${event}`);
        }
        if (retry) {
            raw.push(`retry: ${retry}`);
        }
        if (id) {
            raw.push(`id: ${id}`);
        }

        // Set each event data line.
        raw = raw.concat(data.split(/\n/).map(line => `data: ${line}`)).map(line => `${line}\n`);
        // send the event
        raw[raw.length - 1] = `${raw[raw.length - 1]}\n`;
        return p.then(() => P.each(raw, this._write.bind(this)));
    }


    /**
     * Ends the response.
     */
    end() {
        if (this.res.finished) {
            return P.resolve();
        }
        return new P((resolve, reject) => {
            this.res.end(resolve);
        });
    }


    /**
     * Write the given data to the underlying stream and return
     * only once it has been flushed.
     */
    _write(data) {
        if (data.length === 0) {
            return P.resolve();
        }
        if (this.res.finished) {
          return P.reject('Cannot write events after closing the response!');
        }
        return new P((resolve, reject) => {
            if (!this.res.write(data)) {
                this.res.once('drain', resolve);
            } else {
                process.nextTick(resolve);
            }
        });
    }
}


/**
 * Default serialize function for SSE data and id.
 * This does JSON.stringify(d) if d is not already a string.
 */
function toJSON(d) {
    if (!_.isString(d)) {
        d = JSON.stringify(d);
    }

    return d;
}

module.exports = SSEResponse;
