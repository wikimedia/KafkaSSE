'use strict';

/**
 * An ExtendableError class.
 * This will define a non enumerable this.name property that matches
 * the class name during construction.
 * The constructor takes either an error message string, or another
 * Error object.  If an Error object is given, the Error's enumerable
 * keys will be placed on this new ExtenableError, and a non enumerable
 * this.originalName will be set to the class name of the provided Error.
 */
class ExtendableError extends Error {
    /**
     * @param {string|Error} e
     * @constructor
     */
    constructor(e) {
        // Allow errors to be instantied by wrapping other errors.
        if (e instanceof Error) {
            // message is an Error, instantiate this with the message string.
            super(e.message);
            // Copy each of the Error's properties to this.
            Object.keys(e).forEach((k) => {
                this[k] = e[k];
            });
            Object.defineProperty(this, 'originalName', {
                value:          e.name,
                configurable:   true,
                enumerable:     false,
                writable:       true,
            });
        }
        // Otherwise make a new Error with message.
        else {
            super(e);
        }

        Error.captureStackTrace(this, this.constructor);

        Object.defineProperty(this, 'name', {
            value:          this.constructor.name,
            configurable:   true,
            enumerable:     false,
            writable:       true,
        });
    }
}


/**
 * An ExtendableError class that always sets this.origin to 'KafkaSSE',
 * and provides a toJSON method for easy serialization.
 */
class KafkaSSEError extends ExtendableError {
    /**
     * Creates a new KafkaSSEError with extraPropeties
     * defined.
     *
     * @param {string|Error} e
     * @param {Object extraProperties}
     * @constructor
     */
    constructor(e, extraProperties) {
        super(e);

        Object.defineProperty(this, 'origin', {
            value:          'KafkaSSE',
            configurable:   true,
            enumerable:     false,
            writable:       true,
        });

        // Copy each extra property to this.
        if (extraProperties) {
            Object.keys(extraProperties).forEach((k) => {
                this[k] = extraProperties[k];
            });
        }
    }

    /**
     * Returns a JSON.stringify-able object.
     * This will include properties defined by
     * ExtendableError and KafkaSSEError, as well
     * as any extra enumerable properties that have been set.
     *
     * @return {Object}
     */
    toJSON() {
        let obj = {};
        // Include these non enumerable keys and all enumerable keys
        // on the object that will be used for JSON serialization.
        ['message', 'origin', 'name', 'originalName', 'stack']
        .concat(Object.keys(this))
        .forEach(k => {
            if (k in this) {
                obj[k] = this[k];
            }
        });

        return obj;
    }
}


class ConfigurationError        extends KafkaSSEError { }
class InvalidAssignmentError    extends KafkaSSEError { }
class TopicNotAvailableError    extends KafkaSSEError { }
class DeserializationError      extends KafkaSSEError { }



module.exports = {
    ExtendableError:            ExtendableError,
    KafkaSSEError:              KafkaSSEError,
    ConfigurationError:         ConfigurationError,
    InvalidAssignmentError:     InvalidAssignmentError,
    TopicNotAvailableError:     TopicNotAvailableError,
    DeserializationError:       DeserializationError,
};
