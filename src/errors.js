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
 * Basic error handling helpers
 */
export class ErrorHandler {
  static throwError(err) {
    if (!err) {
      err = new Error('An error occurred');
    }

    if (_.isString(err)) {
      throw new Error(err);
    }

    throw err;
  }

  static rejectError(err) {
    return Promise.reject(err);
  }

  static logInfo(module, info) {
    if (process.env.NODE_ENV === 'development' ||
        process.env.NODE_ENV === 'staging') {
      console.info(chalk.cyan(module + ':', info));
    }
  }

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

  static logErrorAndExit(err, context, help) {
    ErrorHandler.logError(err, context, help);
    process.exit(0);
  }

  static logAndThrowError(err, context, help) {
    ErrorHandler.logError(err, context, help);
    ErrorHandler.throwError(err, context, help);
  }

  static logAndRejectError(err, context, help) {
    ErrorHandler.logError(err, context, help);
    return ErrorHandler.rejectError(err, context, help);
  }
};
