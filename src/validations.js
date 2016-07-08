// Libraries
import _ from 'lodash';
import check from 'check-types';
// Modules
import { errors } from './errors';
import * as helpers from './helpers';
import Schema from './schema';

/**
 * @typedef Validator
 * @type {object}
 * @property {string} [displayName] The display name of the validator
 * @property {Array<function(*): boolean>} validator The array of validation functions
 * @property {*} [sanitizer] Optional function to sanitze the value before validating it
 */

export default class Validations {
  /**
   * Handler for validations against a specific schema.
   * @param {Schema} schema The model's schema
   * @param {Object<string, Validator>} definition The validation
   *  definition which is a map, where every key refers to a field name and the value is validator, which is an object
   *  containing a validator function and display message function.
   * @class Validations
   */
  constructor(schema, definition) {
    /* type-check */
    check.assert.instanceStrictOf(schema, Schema);
    check.assert.object(definition);
    /* end-type-check */
    /**
     * @type {Schema}
     * @name schema
     * @private
     * @memberOf Validations
     */
    this.schema = schema;
    /**
     * @type {!Object}
     * @name definition
     * @private
     * @memberOf Validations
     */
    this.definition = definition;

    this.validateAndNormalizeDefinition(definition);
  }

  /**
   * Helper method to validate the validation definition.
   *
   * @param {Object<string, Validator>} definition The validation
   *  definition which is a map, where every key refers to a field name and the value is validator, which is an object
   *  containing a validator function and display message function.
   * @public
   * @throws {InvalidColumnError}
   * @throws {InvalidTypeError}
   * @throws {InvalidValidationDefinitionKeyError}
   * @function validateAndNormalizeDefinition
   * @memberOf Validations
   * @instance
   */
  validateAndNormalizeDefinition(definition) {
    /* type-check */
    check.assert.object(definition);
    /* end-type-check */
    _.each(definition, (recipe, column) => {
      if (!this.schema.isColumn(column)) {
        throw new errors.InvalidColumnError(`Invalid column: ${column}.`);
      }

      _.each(recipe, (value, key) => {
        if (key === 'displayName') {
          if (!_.isString(value)) {
            throw new errors.InvalidTypeError('Type should be a string.');
          }
        } else if (key === 'validator') {
          // normalize
          if (helpers.isPlainObject(value)) {
            value = [value];
            recipe[key] = value;
          }

          // validate
          if (!_.isArray(value)) {
            throw new errors.InvalidTypeError('Type should be an array.');
          } else {
            _.each(value, (v, index) => {
              if (!helpers.isPlainObject(v)) {
                throw new errors.InvalidTypeError('Type should be an object.');
              } else if (!v.validator || !_.isFunction(v.validator)) {
                throw new errors.InvalidTypeError('Type should be a function.');
              } else if (!v.message || !_.isFunction(v.message)) {
                throw new errors.InvalidTypeError('Type should be a function.');
              }
            });
          }
        } else if (key === 'sanitizer') {
          if (_.isFunction(value)) {
            value = [value];
            recipe[key] = value;
          } else if (!_.isArray(value)) {
            throw new errors.InvalidTypeError('Type should be an array.');
          } else {
            _.each(value, (v, index) => {
              if (!_.isFunction(v)) {
                throw new errors.InvalidTypeError('Type should be a function.');
              }
            });
          }
        } else {
          throw new errors.InvalidValidationDefinitionKeyError(`Unknown validation definition key: ${key}.`);
        }
      });
    });
  }

  /**
   * Fetches the validation for a given column.
   *
   * @param {string} column The name of the column
   * @return {Object} The Validation object if one exists, otherwise an empty object
   * @public
   * @function getValidation
   * @memberOf Validations
   * @instance
   */
  getValidation(column) {
    if (this.definition[column]) {
      return this.definition[column];
    } else {
      return {};
    }
  }

  /**
   * @ignore
   */
  mixin(model) {}

  /**
   * Validates a given value based on a specific validator.
   *
   * @param {Validator} recipe The Validator to apply
   * @param {*} value The value to be validated
   * @param {string} displayName The display name of the column/field
   * @param {Orm} orm An instance of the ORM
   * @throws {InvalidTypeError}
   * @public
   * @function validate
   * @memberOf Validations
   * @static
   */
  static validate(recipe, value, displayName, orm) {
    /* type-check */
    check.assert.object(recipe);
    check.assert.string(displayName);
    /* end-type-check */
    let messages = null;

    if (recipe.validator) {
      let validators = recipe.validator;
      if (helpers.isPlainObject(validators)) {
        validators = [validators];
      }

      if (_.isArray(validators)) {
        _.each(validators, (validator) => {
          if (!validator.validator(value, orm)) {
            if (!messages) {
              messages = [];
            }

            const message = validator.message(displayName);
            if (_.isArray(message)) {
              messages = messages.concat(message);
            } else {
              messages.push(message);
            }
          }
        });
      } else {
        throw new errors.InvalidTypeError('Type should be an array.');
      }
    }

    return messages;
  }

  /**
   * Sanitizes a given value based on a specific validator.
   *
   * @param {Validator} recipe The Validator to apply
   * @param {*} value The value to be validated
   * @param {Orm} orm An instance of the ORM
   * @return {*} The sanitized value
   * @throws {InvalidTypeError}
   * @public
   * @function sanitize
   * @memberOf Validations
   * @static
   */
  static sanitize(recipe, value, orm) {
    /* type-check */
    check.assert.object(recipe);
    /* end-type-check */
    if (recipe.sanitizer) {
      let sanitizers = recipe.sanitizer;
      if (_.isFunction(sanitizers)) {
        sanitizers = [sanitizers];
      }

      if (_.isArray(sanitizers)) {
        return _.reduce(sanitizers, (value, sanitizer) => { return sanitizer(value, orm); }, value);
      } else {
        throw new errors.InvalidTypeError('Type should be an array.');
      }
    }
    return value;
  }

  /**
   * Sanitizes and then validates a given value based on a specific validator.
   *
   * @param {Validator} recipe The Validator to apply
   * @param {*} value The value to be validated
   * @param {string} displayName The display name of the column/field
   * @param {Orm} orm An instance of the ORM
   * @return {*} The result of the validation
   * @throws {InvalidTypeError}
   * @public
   * @function sanitize
   * @memberOf Validations
   * @static
   */
  static validateSanitized(recipe, value, displayName, orm) {
    /* type-check */
    check.assert.object(recipe);
    check.assert.string(displayName);
    /* end-type-check */
    return Validations.validate(recipe, Validations.sanitize(recipe, value, orm), displayName, orm);
  }
}
