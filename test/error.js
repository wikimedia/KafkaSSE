'use strict';


// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

const assert = require('assert');

const errors                 = require('../lib/error.js');
const ExtendableError        = errors.ExtendableError;
const KafkaSSEError           = errors.KafkaSSEError;


describe('ExtendableError', () => {
    it('should construct using string', function() {
        let m = 'error string';
        let ee = new ExtendableError(m);

        assert.equal(ee.message, m);
        assert.equal(ee.name, ExtendableError.name);
    });

    it('should construct using Error', function() {
        let m = 'error string';
        let e = new Error(m)
        let ee = new ExtendableError(e);

        assert.equal(ee.message, e.message);
        assert.equal(ee.name, ExtendableError.name);
        assert.equal(ee.originalName, e.constructor.name);
    });

    it('should construct using Error with properties', function() {
        let m = 'error string';
        let e = new Error(m)
        e.prop1 = 'a property value';
        let ee = new ExtendableError(e);

        assert.equal(ee.prop1, e.prop1, 'enumerable properties should be present');
    });
});


describe('KafkaSSEError', () => {
    it('should construct and set origin', function() {
        let m = 'error string';
        let ke = new KafkaSSEError(m);

        assert.equal(ke.origin, 'KafkaSSE', 'KafkaSSEError.origin should be set to KafkaSSE');
    });

    it('should construct with extra properties', function() {
        let m = 'error string';
        let extra = { 'prop1': 'a property value' };
        let ke = new KafkaSSEError(m, extra);

        assert.equal(ke.prop1, extra.prop1);
    });

    it('should serialize to JSON and back with extra properties', function() {
        let m = 'error string';
        let extra = { 'prop1': 'a property value' };
        let ke = new KafkaSSEError(m, extra);

        let deserializedError = JSON.parse(JSON.stringify(ke));
        assert.equal(deserializedError.message, ke.message);
        assert.equal(deserializedError.name, ke.name);
        assert.equal(deserializedError.prop1, ke.prop1);
    })
});
