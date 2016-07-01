import _ from 'lodash';
import chalk from 'chalk';
import path from 'path';
import Promise from 'bluebird';

const prefix = 'priam.orm.error.';

function compileErrors(prefix, namespace, errors) {
  _.each(errors, (error, index) => {
    class CustomError extends Error {
      constructor(message) {
        super(message);

        this.name = prefix + error;
        this.message = message;
        this.errorType = error;

        this.stack = (new Error(message)).stack;
      }
    };
    namespace[error] = CustomError;
  });
}

/**
 *
 * 
 */
export let errors = {};
compileErrors(prefix, errors, [
  'ValidationError',
  'TokenRevocationError',
  'NoPermissionError',
  'UnauthorizedError',
  'BadRequestError',
  'InvalidColumnError',
  'InvalidTypeError',
  'InvalidArgumentError',
  'InvalidValidationDefinitionKeyError',
  'SelectSchemaError',
  'CreateError',
  'FixError',
  'InvalidValidationDefinitionKeyError'
]);

/**
 * Error handler for priam. Provides the unique Error type as well
 * as common functionality for handling errors in the subsystems.
 * @class
 */
export class ErrorHandler {
  /**
   * Throws a given error message.
   *
   * @param {string} [err] The error message, defaults to 'An error occurred'
   * @public
   * @static
   */
  static throwError(err) {
    if (!err) {
      err = new Error('An error occurred');
    }

    if (_.isString(err)) {
      throw new Error(err);
    }

    throw err;
  }

  /**
   * Rejects a promise with a given error message.
   *
   * @param {string} [err] The error message, defaults to 'An error occurred'
   * @public
   * @static
   */
  static rejectError(err) {
    if (!err) {
      err = new Error('An error occurred');
    }

    return Promise.reject(err);
  }

  /**
   * Logs informative text to the console depending
   * on the ORM level settings.
   *
   * @param {string} module The module where the error occurred
   * @param {string} info The information to log
   * @public
   * @static
   */
  static logInfo(module, info) {
    if (process.env.NODE_ENV === 'development' ||
        process.env.NODE_ENV === 'staging') {
      console.info(chalk.cyan(module + ':', info));
    }
  }

  /**
   * Logs warning text to the console depending
   * on the ORM level settings.
   *
   * @param {string} warn The warning message to log
   * @param {string} [context] Additional context such as corrective
   *                           action or versioning information
   * @public
   * @static
   */
  static logWarn(warn, context) {
    if (process.env.NODE_ENV === 'development' ||
        process.env.NODE_ENV === 'staging') {
        warn = warn || 'no message supplied';
      let msgs = [chalk.yellow('\nWarning:', warn), '\n'];

      if (context) {
        msgs.push(chalk.white(context), '\n');
      }

      // add a new line
      msgs.push('\n');

      console.log.apply(console, msgs);
    }
  }

  /**
   * Logs an error to the console with a the resulting stacktrace
   * depending on the ORM level settings.
   *
   * @param {Error|string|Array<string>} err The error to log
   * @param {string} [context] Additional context such as corrective
   *                           action or versioning information
   * @public
   * @static
   */
  static logError(err, context) {
    const origArgs = _.toArray(arguments).slice(1);
    let stack;
    let msgs;

    if (_.isArray(err)) {
      _.each(err, (e) => {
        const newArgs = [e].concat(origArgs);
        errors.logError.apply(this, newArgs);
      });
      return;
    }

    stack = err ? err.stack : null;

    if (!_.isString(err)) {
      if (_.isObject(err) && _.isString(err.message)) {
        err = err.message;
      } else {
        err = 'An unknown error occurred.';
      }
    }
    
    // TODO: Logging framework hookup
    // Eventually we'll have better logging which will know about envs
    if ((process.env.NODE_ENV === 'development' ||
        process.env.NODE_ENV === 'staging' ||
        process.env.NODE_ENV === 'production')) {
      msgs = [chalk.red('\nERROR:', err), '\n'];

      if (context) {
        msgs.push(chalk.white(context), '\n');
      }

      // add a new line
      msgs.push('\n');

      if (stack) {
        msgs.push(stack, '\n');
      }

      console.error.apply(console, msgs);
    }
  }

  /**
   * Logs an error to the console and then exits the process.
   *
   * @param {Error|string|Array<string>} err The error to log
   * @param {string} [context] Additional context such as corrective
   *                           action or versioning information
   * @public
   * @static
   */
  static logErrorAndExit(err, context) {
    ErrorHandler.logError(err, context);
    process.exit(0);
  }

  /**
   * Logs an error to the console and throws as a new Error.
   *
   * @param {Error|string|Array<string>} err The error to log
   * @param {string} [context] Additional context such as corrective
   *                           action or versioning information
   * @public
   * @static
   */
  static logAndThrowError(err, context) {
    ErrorHandler.logError(err, context);
    ErrorHandler.throwError(err);
  }

  /**
   * Logs an error to the console and rejects the promise with the error.
   *
   * @param {Error|string|Array<string>} err The error to log
   * @param {string} [context] Additional context such as corrective
   *                           action or versioning information
   * @public
   * @static
   */
  static logAndRejectError(err, context) {
    ErrorHandler.logError(err, context);
    return ErrorHandler.rejectError(err);
  }
};
