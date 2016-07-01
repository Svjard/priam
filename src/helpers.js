// Libraries
import _ from 'lodash';
import cassandra from 'cassandra-driver';
import * as validator from 'validator';

const cassandraTypes = cassandra.types;

/**
 * Checks for a javascript object.
 *
 * @param {*} x Value to check against
 * @return {boolean}
 * @module helpers
 */
export function isPlainObject(x) {
  return _.isPlainObject(x);
}

/**
 * Checks for an Integer.
 *
 * @param {*} x Value to check against
 * @return {boolean}
 * @module helpers
 */
export function isInteger(x) {
  return validator.isInt(x.toString());
}

/**
 * Checks for a javascript Date object.
 *
 * @param {*} x Value to check against
 * @return {boolean}
 * @module helpers
 */
export function isDateTime(x) {
  return validator.isDate(x.toString());
}

/**
 * Helper method that always returns true
 *
 * @param {*} x
 * @return {boolean}
 * @module helpers
 */
export function isAnything(x) {
  return true;
}

/**
 * Checks for a valid UUID string
 *
 * @param {*} x Value to check against
 * @return {boolean}
 * @module helpers
 */
export function isUUID(x) {
  var pattern_uuid1 = /^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$/i;
  return pattern_uuid1.test(x.toString());
}

/**
 * Checks for a valid internet address string (IPv4, IPv6 both supported)
 *
 * @param {*} x Value to check against
 * @return {boolean}
 * @module helpers
 */
export function isInet(x) {
  if (!_.isString(x)) {
    return false;
  }
  else {
    const patt_ip4 = /^(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}$/i;
    const patt_ip6_1 = /^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$/i;
    const patt_ip6_2 = /^((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)::((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)$/i;
    const patt_ip6_3 = /^::ffff:(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}$/i;
    return patt_ip4.test(x) || patt_ip6_1.test(x) || patt_ip6_2.test(x) || patt_ip6_3.test(x);
  }
}

/**
 * Checks for a valid Tuple as an array of Cassandra tuple instance
 *
 * @param {*} x Value to check against
 * @return {boolean}
 * @module helpers
 */
export function isTuple(x) {
  return (_.isArray(x) && x.length === 3) || (x instanceof cassandraTypes.Tuple);
}

/**
 * Checks if two values are exactly equal.
 *
 * @param {*} x
 * @param {*} y
 * @return {boolean}
 * @module helpers
 */
export function isEqual(x, y) {
  if (_.isArray(x) || isPlainObject(x)) {
    return JSON.stringify(x) === JSON.stringify(y);
  }
  else {
    return x == y;
  }
}

/**
 * Checks if all values in an array are unique.
 *
 * @param {Array} array Array to check against
 * @return {boolean}
 * @module helpers
 */
export function uniq(array) {
  let hash = {};
  _.each(array, (value, index) => {
    const key = JSON.stringify(value);
    if (!hash[key]) {
      hash[key] = value;
    }
  });

  return nm_.values(hash);
}

/**
 * Checks if the value specified is not in a given array.
 *
 * @param {Array} array Array to check against
 * @param {*} value
 * @return {boolean}
 * @module helpers
 */
export function without(array, value) {
  let newArray = [];
  _.each(array, (v, index) => {
    if (!exports.isEqual(v, value)) {
      newArray.push(v);
    }
  });
  return newArray;
}
