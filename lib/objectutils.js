'use strict';

/**
 * Utility functions for working with objects.
 * Includes dotted string lookup, deserialization
 * from JSON in utf-8 buffer, sub-object
 * comparision, etc.
 */

const P           = require('bluebird');
const _           = require('lodash');
const isSafeRegex = require('safe-regex');


/**
 * Converts a utf-8 byte buffer or a JSON string into
 * an object and returns it.
 */
function factory(data) {
    // if we are given an object Object, no-op and return it now.
    if (_.isPlainObject(data)) {
        return data;
    }

    // If we are given a byte Buffer, parse it as utf-8
    if (data instanceof Buffer) {
        data = data.toString('utf-8');
    }

    // If we now have a string, then assume it is a JSON string.
    if (_.isString(data)) {
        data = JSON.parse(data);
    }

    else {
        throw new Error(
            'Could not convert data into an object.  ' +
            'Data must be a utf-8 byte buffer or a JSON string'
        );
    }

    return data;
}


/**
 * Given a string dottedKey, like 'a.b.c',
 * This will return the value in obj
 * addressed by that key.  E.g. this[a][b][c].
 * Taken from http://stackoverflow.com/a/6394168/555565.
 *
 * If the dottedKey does not exist in obj, this will return undefined.
 */
function dot(obj, dottedKey) {
    return dottedKey.split('.').reduce(
        (o,i) => {
            // If we encounter a key that does not exist,
            // just returned undefined all the way back up.
            if (o === undefined) {
                return undefined;
            }
            return o[i];
        },
        obj
    );
}


/**
 * filters should be a flat object with dotted key value pairs.
 * Each dotted key will be looked up in obj and compared
 * with the filter value.  If all given filters match, this will return true,
 * otherwise false.  A filter key is a dotted notation key lookup,
 * and a value must either be a literal value, a RegExp, or an Array
 * of literal values to match against.  If Array filter is given and the
 * dotted key finds a literal value in obj, then obj will match if it
 * filter has a value that matches obj. If Array filter is given and
 * and the dotted key finds an Array in obj, then if all values in the subject
 * are in the filter, match will succeed.
 *
 * This will not match entire objects, only literal values.
 *
 * @param {Object} obj to match against
 * @Param {filters} TODO: document
 * @return {boolean}
 */
function match(obj, filters) {

    // if we don't have any filters, just return true;
    if (_.isEmpty(filters)) {
        return true;
    }

    // Check that every filter in filters returns true.
    return Object.keys(filters).every(key => {
        let subject = dot(obj, key);

        // make sure we aren't targeting a non literal or an Array to match
        // against. If we are, just return false now.
        if (_.isObject(subject) && !_.isArray(subject)) {
            return false;
        }

        let filter = filters[key];

        // If this filter is an Array...
        if (_.isArray(filter)) {
            // ...and the subject is an Array,
            // then match if every item in filter is also in subject.
            if (_.isArray(subject)) {
                return filter.every(f => _.includes(subject, f));
            }
            // If subject is not an array, then match if subject
            // is equal to one of the given filters.
            else {
                return _.includes(filter, subject);
            }
        }

        // If this filter is a RegExp, then test it against the subject.
        // if it matches, then return true now.
        if (filter instanceof RegExp && filter.test(subject)) {
            return true;
        }

        // Otherwise just compare the filter against the subject.
        // If it matches return true now.
        else if (filter === subject) {
            return true;
        }

        // If we get this far we didn't match.  Return false.
        return false;
    });
}



/**
 * Validates filters.  If any filters start and end with '/', they will
 * be converted into RegExps.
 *
 * @param {Object} filters: TODO document
 * @param {boolean} safeRegexes:  If true, safe-regex will be used to check a regex.
 *                  If the regex is not safe, then an Error will be thrown.
 *                  If this parameter is false, no checking will be done.
 * @throws Error if filters do not validate, or if they fail conversion to RegExp.
 *
 * @return {Object}
 */
function buildFilters(filters, safeRegexes) {
    if (!_.isPlainObject(filters)) {
        let e = new Error('Filters must be an Object of key,value pairs.');
        e.filters = filters;
        throw e;
    }

    if (safeRegexes === undefined) {
        safeRegexes = true;
    }

    Object.keys(filters).forEach((key) => {
        const filter = filters[key];

        // Filters must be either strings or numbers.
        if (!_.isArray(filter) && _.isObject(filter)) {
            let e = new Error(`Invalid filter for '${key}', cannot filter using an object.`);
            e.filter = { [key]: filter };
            throw e;
        }

        // If this filter is a regex, it will begin and end with /
        if (_.isString(filter) &&
            filter.charAt(0) === '/'   &&
            filter.charAt(filter.length - 1) === '/'
        ) {
            // Convert this to a regex and save it in filters.
            try {
                filters[key] = new RegExp(filter.substring(1, filter.length - 1));
            }
            catch (e) {
                let e = new Error(e.message);
                e.filter = { [key]: filter };
                throw e;
            }

            // If configured to do so, throw an Error if regex is not safe.
            if (safeRegexes && !isSafeRegex(filters[key])) {
                let e = new Error(`Cannot use provided regex for '${key}', it is not safe.`);
                e.filter = { [key]: filter };
                throw e;
            }
        }
    });

    return filters;
}


/**
 * This takes an object representing filters for the match function,
 * calls buildFilters on it, and then returns a new function that
 * uses the result of buildFilters to match against objects.
 *
 * Example:
 *   var filters = { 'my.key': ['value1', 'value2'], 'other': '/(reg|ex)/' };
 *   var matcher = createMatcherFunctionWithFilters(filters);
 *   var obj = { my: { key: 'value2'}, other: 'This will match a reg ex' };
 *   if (matcher(obj)) {
 *      console.log('It matched!', obj);
 *   }
 *
 *  You can access the actual filters used to match using matcher.filters.
 *
 * @param  {Object}     filters to pass to buildFilters
 * @param  {boolean}    safeRegexes to pass to build Filters
 * @return {function}
 */
function createFilterMatcherFunction(filters, safeRegexes) {
    filters = buildFilters(filters, safeRegexes);
    let matcher = (obj) => {
        return match(obj, filters);
    };
    matcher.filters = filters;
    return matcher;
}

module.exports = {
    factory:                            factory,
    dot:                                dot,
    match:                              match,
    buildFilters:                       buildFilters,
    createFilterMatcherFunction:        createFilterMatcherFunction,
};
