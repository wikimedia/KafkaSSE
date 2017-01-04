'use strict';

const _            = require('lodash');

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
        // Merge optional extra response headers with default ones.
        this.headers = Object.assign({
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive'
            },
            this.options.headers
        );
    }


    /**
     * Starts sending the SSE response body.
     */
    start() {
        this.res.writeHead(200, this.headers);
        this.res.write(':ok\n\n');
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
        // Start the text/event-stream response if it hasn't already started.
        if (!this.res.headersSent) {
            this.start();
        }

        if (!event) {
            throw new Error('Cannot send SSE event.  Must provide event name.');
        }
        if (!data) {
            throw new Error('Cannot send SSE event.  Must provide event data.');
        }

        let senderObject = {
            event : event,
            data  : data,
            id    : id    || undefined,
            retry : retry || undefined
        };

        // Serialize the data and the id, since those may be complex
        // objects interpreted by EventSource clients.
        // (event and retry are not objects.)
        senderObject.data = this.serialize(senderObject.data);
        senderObject.id    = this.serialize(senderObject.id);

        // Send the event headers (event name, retry, id)
        if (senderObject.event) {
            this.res.write('event: ' + senderObject.event + '\n');
        }
        if (senderObject.retry) {
            this.res.write('retry: ' + senderObject.retry + '\n');
        }
        if (senderObject.id) {
            this.res.write('id: ' + senderObject.id + '\n');
        }

        // Replace any consecutive occurences of \r or \n with a single \n,
        // so the data doesn't accidentally end the event prematurely.
        senderObject.data = senderObject.data.replace(/[\r\n]+/g, '\n');

        // Send each event data line.
        const dataLines = senderObject.data.split(/\n/);
        const dataLineCount = dataLines.length;
        for (let i = 0; i < dataLineCount; i++) {
            // If this is our final line of data, end this event with 2 newlines.
            if (i === dataLineCount - 1) {
                this.res.write('data: ' + dataLines[i] + '\n\n');
            }
            // Else we expect more data lines in this event, end this line with a single newline.
            else {
                this.res.write('data: ' + dataLines[i] + '\n');
            }
        }
    }


    /**
     * Ends the response.
     */
    end() {
        if (!this.res.finished) {
            this.res.end();
        }
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
