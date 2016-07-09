// Libraries
import _ from 'lodash';
import * as validator from 'validator';

/**
 * @namespace recipes.validators
 */

/**
 * Handles validating email addresses.
 * @const
 * @memberOf recipes.validators
 */
export const email = {
  validator: (value, instance) => {
    return _.isString(value) && validator.isEmail(value);
  },
  message: (displayName) => { return `${displayName} must be a valid email address.`; }
};

/**
 * Handles validating strong passwords.
 * @const
 * @memberOf recipes.validators
 */
export const password = {
  validator: (value, instance) => {
    return _.isString(value) && validator.matches(value, /^(?=.*[a-zA-Z])(?=.*[0-9]).{6,}$/);
  },
  message: (displayName) => { return `${displayName} must be at least 6 characters long and contain one number and one letter.`; }
};

/**
 * Handles validating value is non-null.
 * @const
 * @memberOf recipes.validators
 */
export const required = {
  validator: (value, instance) => {
    return !!value;
  },
  message: (displayName) => { return `${displayName} is required.`; }
};

/**
 * Generates a validator that checks for a value being within an array of values.
 *
 * @param {Array} values An array of values which must contain the value specified
 * @return {Object}
 * @memberOf recipes.validators
 */
export function isIn(values) {
  return {
    validator: (value, instance) => {
      if (_.isArray(values)) {
        return values.indexOf(value) > -1;
      }
      else {
        return !!values[value];
      }
    },
    message: (displayName) => {
      const displayNames = _.isArray(values) ? values : _.values(values);
      return `${displayName} must have one of these values: ${displayNames.join(', ')}.`;
    }
  }
}

/**
 * Generates a validator that checks for at least value being present in a set of columns
 *
 * @param {Array} columns An array of columns (i.e. fields)
 * @return {Object}
 * @memberOf recipes.validators
 */
export function requiresOneOf(columns) {
  return {
    validator: (value, instance) => {
      let valid = false;
      _.find(columns, (displayName, column) => {
        if (!validator.isNull(instance.get(column))) {
          valid = true;
          return true;
        }
      });
      return valid;
    },
    message: (displayName) => {
      let displayNames = _.values(columns);
      displayNames = _.without(displayNames, displayName);
      return `${displayName} or ${displayNames} is required.`;
    }
  }
}

/**
 * Generates a validator that checks a string or array has a minimum length.
 *
 * @param {number} length The minimum length to check for
 * @return {Object}
 * @memberOf recipes.validators
 */
export function minLength(length) {
  return {
    validator: (value, instance) => {
      if (_.isArray(value)) {
        return value.length >= length;
      }
      else {
        return _.isString(value) && validator.isLength(value, length);
      }
    },
    message: (displayName) => { return `${displayName} is too short (minimum is ${length.toString()} characters).`; }
  }
}

/**
 * Generates a validator that checks a string or array has a maximum length.
 *
 * @param {number} length The maximum length to check for
 * @return {Object}
 * @memberOf recipes.validators
 */
export function maxLength(length) {
  return {
    validator: (value, instance) => {
      if (_.isArray(value)) {
        return value.length <= length;
      }
      else {
        return _.isString(value) && validator.isLength(value, 0, length);
      }
    },
    message: (displayName) => { return `${displayName} is too long (maximum is ${length.toString()} characters).`; }
  }
}

/**
 * Generates a validator that checks a value is greater than or equal a specified value.
 *
 * @param {number} number The value to check against
 * @return {Object}
 * @memberOf recipes.validators
 */
export function greaterThanOrEqualTo(number) {
  return {
    validator: (value, instance) => {
      return _.isNumber(value) && value >= number;
    },
    message: (displayName) => { return `${displayName} is too small (minimum is ${number.toString()}).`; }
  }
}

/**
 * Generates a validator that checks a value is greater than a specified value.
 *
 * @param {number} number The value to check against
 * @return {Object}
 * @memberOf recipes.validators
 */
export function greaterThan(number) {
  return {
    validator: (value, instance) => {
      return _.isNumber(value) && value > number;
    },
    message: (displayName) => { return `${displayName} is too small (must be greater than ${number.toString()}).`; }
  }
}

/**
 * Generates a validator that checks a value is less than or equal a specified value.
 *
 * @param {number} number The value to check against
 * @return {Object}
 * @memberOf recipes.validators
 */
export function lessThanOrEqualTo(number) {
  return {
    validator: (value, instance) => {
      return _.isNumber(value) && value <= number;
    },
    message: (displayName) => { return `${displayName} is too big (maximum is ${number.toString()}).`; }
  }
}

/**
 * Generates a validator that checks a value is less than a specified value.
 *
 * @param {number} number The value to check against
 * @return {Object}
 * @memberOf recipes.validators
 */
export function lessThan(number) {
  return {
    validator: (value, instance) => {
      return _.isNumber(value) && value < number;
    },
    message: (displayName) => { return `${displayName} is too big (must be less than ${number.toString()}).`; }
  }
}

/**
 * Generates a validator that validates a value only if a condition is met
 *
 * @param {Function} conditional Conditional function that should return a truthy value
 * @param {Array} validators The set of validators to run if the conditional is true
 * @return {Object}
 * @memberOf recipes.validators
 */
export function validateIf(conditional, validators) {
  const validateMultiple = validateMultiple(validators);
  return {
    validator: (value, instance) => {
      if (conditional.call(this, value, instance)) {
        return validateMultiple.validator.call(this, value, instance);
      }
      else {
        return true;
      }
    },
    message: (displayName) => { return validateMultiple.message(displayName); }
  }
}

/**
 * Generates a validator that validates a specific field within an object
 *
 * @param {string} field The name of the field in the object
 * @param {string} displayName The verbose name of the field
 * @param {Array} validators The set of validators to run against the field's value
 * @return {Object}
 * @memberOf recipes.validators
 */
export function validateObjectFields(field, displayName, validators) {  
  // normalize
  if (!_.isString(displayName)) {
    validators = displayName;
    displayName = field;
  }
  
  const validateMultiple = validateMultiple(validators);
  return {
    validator: (value, instance) => {
      if (!_.isNull(value)) {
        return validateMultiple.validator.call(this, value[field], instance);
      }
      else {
        return false;
      }
    },
    message: (parentDisplayName) => {
      return {
        field: field,
        messages: validateMultiple.message(displayName)
      };
    }
  }
}

/**
 * Generates a validator that runs a set of validators
 *
 * @param {Array} validators The set of validators to run in serial
 * @return {Object}
 * @memberOf recipes.validators
 */
export function validateMultiple(validators) {
  let failedValidators = null;
  return {
    validator: (value, instance) => {
      // normalize
      if (!_.isArray(validators)) {
        validators = [validators];
      }
      
      failedValidators = _.reduce(validators, (memo, validator) => {
        if (!validator.validator.call(self, value, instance)) {
          memo.push(validator);
        }
        return memo;
      }, []);
      
      return failedValidators.length === 0;
    },
    message: (displayName) => {
      return _.map(failedValidators, (validator) => { return validator.message.call(this, displayName); });
    }
  }
}
