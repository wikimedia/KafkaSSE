'use strict';


// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var assert = require('assert');

var objectutils = require('../lib/objectutils.js');

var o = {
    'a': 'b',
    'o2': {
        'e': 1,
        'r': 'abbbbc',
    },
    'array': ['a', 'b', 'c']
};

describe('factory', () => {
    it('should return same object if given object', () => {
        assert.equal(objectutils.factory(o), o);
    });

    it('should return object from JSON string', () => {
        assert.deepEqual(objectutils.factory(JSON.stringify(o)), o);
    });

    it('should return object from JSON Buffer', () => {
        let buffer = new Buffer(JSON.stringify(o));
        assert.deepEqual(objectutils.factory(buffer), o);
    });

    it('should fail with wrong type', () => {
        assert.throws(objectutils.factory.bind(undefined, 12345));
    });
});

describe('dot', () => {
    it('should lookup values by dotted keys', () => {
        assert.strictEqual(objectutils.dot(o, 'a'), o.a, 'top level lookup');
        assert.strictEqual(objectutils.dot(o, 'o2.e'), o.o2.e, 'dotted lookup');
    });
});


describe('match', () => {
    it('should match literal values based on dotted keys', () => {
        assert.ok(objectutils.match(o, {'a': 'b'}), 'top level lookup should match');
        assert.ok(!objectutils.match(o, {'o2.e': 2}), 'dotted lookup should match');
        assert.ok(!objectutils.match(o, {'not.a.key': 2}), 'undefined lookup should not match');
    });

    it('should strictly match values based on dotted keys', () => {
        assert.ok(objectutils.match(o, {'o2.e': 1}), 'literal value should match');
        assert.ok(!objectutils.match(o, {'o2.e': "1"}), 'string vs number should not match');
    });

    it('should match regexes against strings based on dotted keys', () => {
        assert.ok(objectutils.match(o, {'o2.r': /^ab+c/}), 'regex should match');
        assert.ok(!objectutils.match(o, {'o2.r': /^b.*$/}), 'regex should not match');
    });

    it('should match regexes against strings and literals based on dotted keys', () => {
        assert.ok(objectutils.match(o, {'o2.r': /^ab+c/, 'a': 'b'}), 'regex and literal should match');
        assert.ok(!objectutils.match(o, {'o2.r': /^b.*$/, 'a': 'c'}), 'regex should match but literal should not');
    });

    it('should not match object filter', () => {
        assert.ok(!objectutils.match(o, {'a': {'no': 'good'}}), 'cannot match with object as filter');
    });

    it('should not match object as key target', () => {
        assert.ok(!objectutils.match(o, {'o2': 'nope'}), 'cannot match with object as key target');
    });

    it('should inclusive match an array', () => {
        assert.ok(objectutils.match(o, {'array': ['a']}), 'should match array value');
        assert.ok(objectutils.match(o, {'array': ['a', 'b']}), 'should match array values');
        assert.ok(!objectutils.match(o, {'array': ['a', 'd']}), 'should not match array values that are not in array');
        assert.ok(!objectutils.match(o, {'array': 1}), 'should not array match int');
        assert.ok(!objectutils.match(o, {'array': 'a'}), 'should not array match string');
        assert.ok(!objectutils.match(o, {'array':  {'already': 'tested'}}), 'should not array match obj');
    });

    it('should match a literal in an array of possibilities', () => {
        assert.ok(objectutils.match(o, {'a': ['a', 'b']}), 'should match array of possiblities');
        assert.ok(!objectutils.match(o, {'a': ['d', 'e']}), 'should not match of possibilities');
    });
});


describe('buildFilters', () => {
    it('should build a simple filter', () => {
        let filters = {'a.b.c': 1234};
        let built = objectutils.buildFilters(filters);
        assert.deepEqual(built, filters);
    });

    it('should build a regex filter', () => {
        let filters = {'a.b.c': '/(woo|wee)/'};
        let built = objectutils.buildFilters(filters);
        assert.deepEqual(built['a.b.c'], /(woo|wee)/, 'should convert filter to a RegExp');
    });

    it('should build an array filter', () => {
        let filters = {'a.b.c': ['a', 'b']};
        let built = objectutils.buildFilters(filters);
        assert.deepEqual(built, filters);
    });

    it('should fail with a non object', () => {
        let filters = 'gonna fail dude';
        assert.throws(objectutils.buildFilters.bind(undefined, filters));
    });

    it('should fail with a non string or number filter', () => {
        let filters = {'a.b.c': {'no': 'good'}};
        assert.throws(objectutils.buildFilters.bind(undefined, filters));
    });

    it('should fail with a bad regex filter', () => {
        let filters = {'a.b.c': '/(dangling paren.../'};
        assert.throws(objectutils.buildFilters.bind(undefined, filters));
    });

    it('should fail with an unsafe regex filter', () => {
        let filters = {'a.b.c': '/(a+){10}/'};
        assert.throws(objectutils.buildFilters.bind(undefined, filters));
    });

    it('should build an unsafe regex filter with safe parameter = false', () => {
        let filters = {'a.b.c': '/(a+){10}/'};
        let built = objectutils.buildFilters(filters, false);
        assert.deepEqual(built['a.b.c'], /(a+){10}/, 'should convert unsafe filter to a RegExp');
    });
});
