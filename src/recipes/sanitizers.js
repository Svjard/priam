// Libraries
import _ from 'lodash';
import validator from 'validator';

/**
 * @namespace recipes.sanitizers
 */

/**
 * Santizes email address to remove whitespace and invalid characters.
 *
 * @param {string} value The email address to santize
 * @return {string}
 * @memberOf recipes.sanitizers
 */
export function email(value, instance) {
  if (validator.isEmail(value)) {
    return validator.normalizeEmail(value);
  } else {
    return validator.trim(value).toLowerCase();
  }
}

/**
 * Helper method to santize an array of strings.
 *
 * @param {Function} sanitizer The santizer function to run against a set of strings
 * @return {string}
 * @memberOf recipes.sanitizers
 */
export function map(sanitizer) {
  return (value, instance) => {
    return _.map(value, (v, index) => {
      return sanitizer(v, instance);
    });
  };
}

/**
 * Santizes any string value to all lowercase.
 *
 * @param {string} value The string to santize
 * @return {string}
 * @memberOf recipes.sanitizers
 */
export function lowercase(value, instance) {
  return value.toLowerCase();
}

/**
 * Santizes any string value to remove whitespace.
 *
 * @param {string} value The string to santize
 * @return {string}
 * @memberOf recipes.sanitizers
 */
export function trim(value, instance) {
  return validator.trim(value);
}
