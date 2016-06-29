// Libraries
import _ from 'lodash';
import cassandra from 'cassandra-driver';
import Promise from 'bluebird';
// Modules
import Bucketing from './bucketing';
import { errors, ErrorHandler } from './errors';
import * as helpers from './helpers';
import Keyspace from './keyspace';
import Model from './model';
import Query from './query';
import Schema from './schema';
import UserDefinedType from './user-defined-type';
import Validations from './validations';
import WrappedStream from './wrapped-stream';

import * as callbackRecipes from './recipes/callbacks';
import * as sanitizerRecipes from './recipes/sanitizers';
import * as validatorRecipes from './recipes/validators';

_.mixin(require('lodash-inflection'));

/**
 * Cassandra-based ORM to handle migrations, upgrades, and models.
 * @class
 */
export default class Orm {
  /**
   * @param {Object} options
   * @constructor
   */
  constructor(options) {
    this.ready = false;
    this.client = null;
    this.queryQueue = {
      system: [],
      execute: [],
      eachRow: [],
      stream: []
    };
    
    this.userDefinedTypes = {};
    this.models = {};
    
    this.keyspace = options.connection.keyspace;
    this.options = this.defaultOptions(options);
  }

  /**
   * Performs a set of queries in batch.
   *
   * @param {Array<string>} queries
   * @return {Promise}
   */
  static batch(queries) {
    return Query.batch(this.client, queries);
  }

  /**
   * Generates a new unique ID.
   *
   * @return {string}
   * @public
   */
  static generateUUID() {
    return cassandra.types.Uuid.random().toString();
  }

  /**
   * Generates a new unique time-based ID.
   *
   * @return {string}
   * @public
   */
  static generateTimeUUID() {
    return cassandra.types.TimeUuid.now().toString();
  }

  /**
   * Generates a date from a TimeUUID instance.
   *
   * @param {TimeUUID|string} timeUUID
   * @return {Date}
   * @public
   */
  static getDateFromTimeUUID(timeUUID) {
    if (_.isString(timeUUID)) {
      timeUUID = cassandra.types.TimeUuid.fromString(timeUUID);
    }

    return timeUUID.getDate();
  }

  /**
   * Generates the epoch offset from a TimeUUID instance.
   *
   * @param {TimeUUID|string} timeUUID
   * @return {number}
   * @public
   */
  static getTimeFromTimeUUID(timeUUID) {
    if (_.isString(timeUUID)) {
      timeUUID = cassandra.types.TimeUuid.fromString(timeUUID);
    }

    return timeUUID.getTime();
  }
  
  /**
   * Converts the current javascript Date to a Cassandra timestamp.
   *
   * @return {Date}
   * @public
   */
  static nowToTimestamp() {
    return Orm.dateToTimestamp(new Date());
  }

  /**
   * Converts a javascript Date to a Cassandra timestamp. Currently
   * acts as an identity function.
   *
   * @param {Date} date
   * @return {Date}
   * @public
   */
  static dateToTimestamp(date) {
    return date;
  }

  /**
   * Determines the default options for the ORM based on a merging of our default
   * settings with the user-defined settings.
   *
   * @param {Object} options
   * @public
   */
  defaultOptions(options) {
    const defaults = {
      keyspace: {
        replication: { 'class': 'SimpleStrategy', 'replication_factor': 1 },
        durableWrites: true,
        ensureExists: {
          run: true, // check if keyspace exists and automatically create it if it doesn't
          alter: false // alter existing keyspace to match replication or durableWrites
        }
      },
      logger: {
        level: 'debug', // log this level and higher [debug < info < warn < error]
        queries: true // log queries
      },
      model: {
        tableName: (modelName) => {
          return _.pluralize(modelName.trim().replace(/([a-z\d])([A-Z]+)/g, '$1_$2').replace(/[-\s]+/g, '_').toLowerCase());
        },
        getterSetterName: (columnName) => {
          return columnName.trim().replace(/\s/g, '_');
        },
        typeSpecificSetterName: (operation, columnName) => {
          let name = columnName.trim().replace(/\s/g, '_');
          name = name.charAt(0).toUpperCase() + str.slice(1)();
          if (operation == 'increment' || operation == 'decrement') {
            return operation + name;
          }
          else {
            return operation + _.singularize(name);
          }
        },
        table: {
          ensureExists: {
            run: true, // check if keyspace exists and automaticcaly create it if it doesn't
            recreate: false, // drop and recreate table on schema mismatch, takes precedence over following options
            recreateColumn: false,  // recreate columns where types don't match schema
            removeExtra: false,  // remove extra columns not in schema
            addMissing: false // add columns in schema that aren't in table
          }
        }
      },
      userDefinedType: {
        ensureExists: {
          run: true,
          recreate: false, // drop and recreate type on schema mismatch, takes precedence over following options
          changeType: false, // change field types to match schema
          addMissing: false // add fields in schema that aren't in type
        }
      }
    };
  
    const mergedOptions = _.extend({}, _.omit(options, 'keyspace', 'logger', 'model', 'userDefinedType'));
    
    if (options.keyspace) {
      mergedOptions.keyspace = _.extend(defaults.keyspace, _.omit(options.keyspace, 'ensureExists'));
      mergedOptions.keyspace.ensureExists = _.extend(defaults.keyspace.ensureExists, options.keyspace.ensureExists);
    }
    else {
      mergedOptions.keyspace = defaults.keyspace;
    }
    
    if (options.logger) {
      mergedOptions.logger = _.extend(defaults.logger, options.logger);
    }
    else {
      mergedOptions.logger = defaults.logger;
    }
    
    if (options.model) {
      mergedOptions.model = _.extend(_.omit(defaults.model, 'table'), _.omit(options.model, 'table'));
      if (options.model.table) {
        mergedOptions.model.table = {};
        mergedOptions.model.table.ensureExists = _.extend(defaults.model.table.ensureExists, options.model.table.ensureExists);
      }
      else {
        mergedOptions.model.table = defaults.model.table;
      }
    }
    else {
      mergedOptions.model = defaults.model;
    }
    
    if (options.userDefinedType) {
      mergedOptions.userDefinedType = _.extend(defaults.userDefinedType, _.omit(options.userDefinedType, 'userDefinedType'));
      mergedOptions.userDefinedType.ensureExists = _.extend(defaults.userDefinedType.ensureExists, options.userDefinedType.ensureExists);
    }
    else {
      mergedOptions.userDefinedType = defaults.userDefinedType;
    }
    
    return mergedOptions;
  }

  /**
   * Initializes the ORM by setting up a connection and running through the models, schemas, UDTs, and
   * settings.
   *
   * @param {Object} userDefinedTypes
   * @public
   */
  init(userDefinedTypes = {}) {
    // proccess user defined types
    this.processUserDefinedTypes(userDefinedTypes, this.options.userDefinedType);
    
    // ensure keyspace exists
    return new Promise((resolve, reject) => {
      this.ensureKeyspace(this.options.keyspace)
        .then(() => {
          // set client
          this.client = new cassandra.Client(this.options.connection);
          this.client.connect((err) => {
            this.processQueryQueue(true);

            // ensure user defined types
            this.ensureUserDefinedTypes(userDefinedTypes, this.options.userDefinedTypes)
              .then(() => {
                this.ready = true;
                this.processQueryQueue(false);
                resolve();
              })
              .catch((err) => {
                reject(err);
              });
          });
        })
        .catch((err) => {
          reject(err);
        });
    });
  }

  /**
   * Creates new user-defined type objects for this instance of the ORM. It does
   * generate them in the client however.
   *
   * @param {Object} userDefinedTypes
   * @param {Object} options
   * @public
   */
  processUserDefinedTypes(userDefinedTypes, options) {
    _.each(userDefinedTypes, (definition, name) => {
      if (this.userDefinedTypes[name]) {
        throw new errors.DuplicateUserDefinedTypeError(i18n.t('errors.orm.DuplicateUserDefinedType', { name: name }));
      }
      else {
        this.userDefinedTypes[name] = new UserDefinedType(this, name, definition, options);
      }
    });
  }

  /**
   * Generates the user-defined types in the Cassandra client based on the specified
   * options.
   *
   * @param {Object} userDefinedTypes
   * @param {Object} options
   * @return {Promise}
   * @public
   */
  ensureUserDefinedTypes(userDefinedTypes, options) {
    let promises = [];
    _.each(this.userDefinedTypes, (userDefinedType, index) => {
      promises.push(
        new Promise((resolve, reject) => {
          userDefinedType.ensureExists((err) => {
            if (err) {
              reject(err);
            } else {
              resolve();
            }
          });
        })
      );
    });
    
    return new Promise((resolve, reject) => {
      Promise.all(promises).then((descriptors) => {
        resolve();
      }).catch((err) => {
        reject(new errors.EnsureUserDefinedTypeExistsError(i18n.t('errors.orm.failedEnsuringUdt')));
      });
    });
  }

  /**
   * Fetches the instance of the UserDefinedType for this ORM based on the name
   * specified.
   *
   * @param {string} name The user-defined type name
   * @return {UserDefinedType}
   * @public
   */
  getUserDefinedType(name) {
    return this.userDefinedTypes[name];
  }

  /**
   * Fetches the instance of the UserDefinedType for this ORM based on the name
   * specified.
   *
   * @param {string} name The user-defined type name
   * @return {Promise}
   * @public
   */
  ensureKeyspace(options) {
    // copy connection contactPoints, ignore keyspace
    let connection = {
      contactPoints: this.options.connection.contactPoints
    };
    
    // create temporary client
    // keyspace needs a client without a keyspace defined
    let client = new cassandra.Client({ contactPoints: connection.contactPoints });
    return new Promise((resolve, reject) => {
      client.connect((err) => {
        const keyspace = new Keyspace(client, this.keyspace, options.replication, options.durableWrites, options);
        keyspace.ensureExists()
          .then(() => {
            // shutdown client since no longer needed
            this.handleShutdown(client)
              .then(() => {
                resolve();
              })
              .catch((err) => {
                ErrorHandler.logError(
                  err,
                  'Failed to shutdown the Cassandra client properly'
                );
                resolve();
              });
          })
          .catch((err) => {
            reject(new errors.EnsureKeyspaceExistsError(i18n.t('errors.orm.failedEnsuringKeyspace')));
          });
      });
    });
  }

  /**
   * Handles the graceful shutdown of a Cassandra client.
   *
   * @param {Client} client The instance of the Cassandra cliet
   * @return {Promise}
   * @public
   */
  handleShutdown(client) {
    if (!(client instanceof cassandra.Client)) {
      return Promise.reject(new errors.Keyspace.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeCassandra')));
    }

    return new Promise((resolve, reject) => {
      client.shutdown((err) => {
        if (err) reject(err);
        resolve();
      });
    });
  }

  /**
   * Adds a model to the ORM for use.
   *
   * @param {string} name The name of the model
   * @param {Object} schema The schema definition for the model's table
   * @param {Object} validation The set of validations to apply to the model's fields
   * @param {Object} options
   * @return {Promise}
   */
  addModel(name, schema, validations, options) {
    if (this.models[name]) {
      Promise.reject(new errors.DuplicateModelError(i18n.t('errors.orm.duplicateModelName')));
    }
    else {
      // default options
      options = _.extend({}, this.options.model, options);

      const schema = new Schema(this, schema, options.schema);
      const validations = validations ? new Validations(schema, validations, options.validations) : null;
      return new Promise((resolve, reject) => {
        Model.compile(this, name, schema, validations, options)
          .then((model) => {
            this.models[name] = model;
            resolve(model);
          });
      });
    }
  }

  /**
   * Returns the model registered with the ORM.
   *
   * @param {string} name The name of the model
   * @return {Model}
   */
  getModel(name) {
    return this.models[name];
  }
  
  /**
   * Runs a given query against the ORM's client.
   *
   * @param {string} query Parameterized query to run
   * @param {Array<*>} params Set of parameters to give to the query
   * @param {Object} options
   * @param {boolean} options.usePrepare Flag inidicating if the query
   *                                     is parameterized
   * @return {Promise}
   */
  execute(query, params, options) {
    return new Promise((resolve, reject) => {
      if (!this.ready) {
        this.addToQueryQueue('execute', arguments);
        resolve();
      }
      else {
        // ErrorHandler.logInfo('Query: ' + query + ', Params: ' + params + ', Options: ' + options);
        this.client.execute(query, params, options, (err, result) => {
          if (err) return reject(err);
          resolve(result);
        });
      }
    });
  }

  /**
   * Iterator function that will apply a possible transform to every
   * row returned from a ResultSet.
   *
   * @param {string} query Parameterized query to run
   * @param {Array<*>} params Set of parameters to give to the query
   * @param {Object} options
   * @param {boolean} options.usePrepare Flag inidicating if the query
   *                                     is parameterized
   * @param {Function} rowTransform
   * @param {Function} completeCallback
   */
  eachRow(query, params, options, rowTransform, completeCallback) {
    if (!this.ready) {
      this.addToQueryQueue.call(this, 'eachRow', arguments);
    }
    else {
      this.client.eachRow(query, params, options, rowTransform, completeCallback);
    }
  }

  /**
   * Generates a readable stream for a given model and query.
   *
   * @param {Model} model The model type
   * @param {string} query Parameterized query to run
   * @param {Array<*>} params Set of parameters to give to the query
   * @param {Object} options
   * @param {boolean} options.usePrepare Flag inidicating if the query
   *                                     is parameterized
   * @return WrappedStream
   */
  stream(model, query, params, options) {
    if (!(_.isFunction(model))) {
      options = params;
      params = query;
      query = model;
      model = null;
    }

    const stream = new WrappedStream(model);
    if (!this.ready) {
      addToQueryQueue.call(this, 'stream', { stream: stream, args: [query, params, options] });
    }
    else {
      stream.setStream(this.client.stream(query, params, options));
    }

    return stream;
  }

  // NEVER call this directly
  /**
   * Internal ORM function that sends a query to the Cassandra client
   * for execution.
   *
   * @param {string} query Parameterized query to run
   * @param {Array<*>} params Set of parameters to give to the query
   * @param {Object} options
   * @param {boolean} options.usePrepare Flag inidicating if the query
   *                                     is parameterized
   * @private
   */
  _system_execute(query, params, options) {
    return new Promise((resolve, reject) => {
      if (!this.client) {
        this.addToQueryQueue('system', arguments);
        resolve();
      }
      else {
        this.client.execute(query, params, options, (err, results) => {
          if (err) {
            reject(err);
          }
          else {
            resolve(results);
          }
        });
      }
    });
  }

  /**
   * Adds a query to the queue to run when the client connection becomes
   * ready.
   *
   * @param {string} action Action type to be run, points to a function name in the ORM
   * @param {Array<*>} any Arguments to pass to the action
   * @private
   */
  addToQueryQueue(action, args) {
    if (!this.queryQueue) {
      throw new errors.QueryQueueAlreadyProcessedError(i18n.t('errors.orm.queryAlreadyProcessed'));
    }
    else if (action !== 'system' && action !== 'execute' && action !== 'eachRow' && action !== 'stream') {
      throw new errors.InvalidQueryQueueActionError(i18n.t('errors.orm.invalidQueueAction'));
    }
    else {
      this.queryQueue[action].push(args);
    }
  }

  /**
   * Processes all the current enqueued queries in the queue.
   *
   * @param {boolean} systemOnly Flag indicating only to run basic queries through _system_execute
   * @private
   */
  processQueryQueue(systemOnly) {
    if (!this.queryQueue) {
      throw new errors.QueryQueueAlreadyProcessedError(i18n.t('errors.orm.queryAlreadyProcessed'));
    }
    else {
      _.each(this.queryQueue, (queue, action) => {
        _.each(queue, (query, index) => {
          if (action === 'system') {
            this._system_execute(query);
          }

          if (!systemOnly) {
            if (action === 'execute') {
              this.execute(query);
            }
            else if (action === 'eachRow') {
              this.eachRow(query);
            }
            else if (action === 'stream') {
              query.stream.setStream(this.client.stream.apply(this, query.args));
            }
          }
        });
      });
      
      if (systemOnly) {
        this.queryQueue.system = [];
      }
      else {
        this.queryQueue = null;
      }
    }
  }
};

Orm.Bucketing = Bucketing;
Orm.Keyspace = Keyspace;
Orm.Model = Model;
Orm.Query = Query;
Orm.Schema = Schema;
Orm.UserDefinedType = UserDefinedType;
Orm.Validations = Validations;

Orm.Recipes = {
  Callbacks: callbackRecipes,
  Sanitizers: sanitizerRecipes,
  Validators: validatorRecipes
};
