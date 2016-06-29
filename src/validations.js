// Libraries
import _ from 'lodash';
// Modules
import { errors } from './errors';
import * as helpers from './helpers';
import Schema from './schema';

/**
 * Handler for validations against a specific schema.
 * @class
 */
export default class Validations {
  /**
   * @param {Object} schema The model's schema
   * @param {Object} definition The validation definition which is a map, where every key refers
   *                            to a field name and the value is validator, which is an object
   *                            containing a validator function and display message function.
   * @constructor
   */
  constructor(schema, definition) {
    this.schema = schema;
    this.definition = definition;

    this.validateAndNormalizeDefinition(definition); // must be called after setting this.definition
  }

  /**
   * Helper method to validate the validation definition.
   *
   * @param {Object} definition The validation definition, see constructor definition for more information
   */
  validateAndNormalizeDefinition(definition) {
    _.each(definition, (recipe, column) => {
      if (!this.schema.isColumn(column)) {
        throw new errors.InvalidColumnError(i18n.t('errors.orm.general.invalidColumn'));
      }

      _.each(recipe, (value, key) => {
        if (key === 'displayName') {
          if (!_.isString(value)) {
            throw new errors.InvalidTypeError(i18n.t('errors.orm.types.shouldBeString'));
          }
        }
        else if (key === 'validator') {
          // normalize
          if (helpers.isPlainObject(value)) {
            value = [value];
            recipe[key] = value;
          }

          // validate
          if (!_.isArray(value)) {
            throw new errors.InvalidTypeError(i18n.t('errors.orm.types.shouldBeArray'));
          }
          else {
            _.each(value, (v, index) => {
              if (!helpers.isPlainObject(v)) {
                throw new errors.InvalidTypeError(i18n.t('errors.orm.types.shouldBeObject'));
              }
              else if (!v.validator || !_.isFunction(v.validator)) {
                throw new errors.InvalidTypeError(i18n.t('errors.orm.types.shouldBeFunction'));
              }
              else if (!v.message || !_.isFunction(v.message)) {
                throw new errors.InvalidTypeError(i18n.t('errors.orm.types.shouldBeFunction'));
              }
            });
          }
        }
        else if (key === 'sanitizer') {
          // normalize
          if (_.isFunction(value)) {
            value = [value];
            recipe[key] = value;
          }
          // validate
          else if (!_.isArray(value)) {
            throw new errors.InvalidTypeError(i18n.t('errors.orm.types.shouldBeArray'));
          }
          else {
            _.each(value, (v, index) => {
              if (!_.isFunction(v)) {
                throw new errors.InvalidTypeError(i18n.t('errors.orm.types.shouldBeFunction'));
              }
            });
          }
        }
        else {
          throw new errors.InvalidValidationDefinitionKeyError(i18n.t('errors.orm.general.unknownDefinitionKey', { key: key }));
        }
      });
    });
  }

  recipe(column) {
    if (this.definition[column]) {
      return this.definition[column];
    }
    else {
      return {};
    }
  }

  mixin(model) {}

  static validate(recipe, value, displayName, instance) {
    let messages = null;
    
    if (recipe.validator) {
      let validators = recipe.validator;
      if (helpers.isPlainObject(validators)) {
        validators = [validators];
      }
      
      if (_.isArray(validators)) {
        _.each(validators, (validator) => {
          if (!validator.validator(value, instance)) {
            if (!messages) {
              messages = [];
            }

            const message = validator.message(displayName);
            if (_.isArray(message)) {
              messages = messages.concat(message);
            }
            else {
              messages.push(message);
            }
          }
        });
      }
      else {
        throw new errors.InvalidTypeError(i18n.t('errors.orm.types.shouldBeArray'));
      }
    }

    return messages;
  }

  static sanitize(recipe, value, instance) {
    if (!helpers.isPlainObject(recipe)) {
      throw new errors.Validations.InvalidType(i18n.t('errors.orm.types.shouldBeObject'));
    }
    else if (recipe.sanitizer) {
      // normalize
      let sanitizers = recipe.sanitizer;
      if (_.isFunction(sanitizers)) {
        sanitizers = [sanitizers];
      }
      
      if (_.isArray(sanitizers)) {
        return _.reduce(sanitizers, (value, sanitizer) => { return sanitizer(value, instance); }, value);
      }
      else {
        throw new errors.Validations.InvalidType(i18n.t('errors.orm.types.shouldBeArray'));
      }
    }
    return value;
  }

  static validateSanitized(recipe, value, displayName, instance) {
    return Validations.validate(recipe, Validations.sanitize(recipe, value, instance), displayName, instance);
  }
}
