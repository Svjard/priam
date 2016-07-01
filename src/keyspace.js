// Libraries
import cassandra from 'cassandra-driver';
import check from 'check-types';
import _ from 'lodash';
// Modules
import { errors, ErrorHandler } from './errors';
import * as helpers from './helpers';
import * as replicationStrategies from './replication-strategies';

/**
 * @interface
 */
const KeyspaceOptions = {
  /**
   * Replication strategy to apply to the keyspace.
   *
   * @name KeyspaceOptions#replication
   */
  replication: ,
  durableWrites: true,
  ensureExists: {
    run: true,
    alter: false
  }
};

/**
 * Cassandra keyspace representation for the ORM.
 * @class
 */
export default class Keyspace {
  /**
   * @param {Client} client The instance of the Cassandra client
   * @param {string} name The name of the keyspace
   * @param {Object} replication The replication class and parameters, see Cassandra documentation
   * @param {boolean} durableWrites Flag indicating if durable writes is set
   * @param {Object} options The options set from the Cassandra config, i.e. config.js
   * @constructor
   */
  constructor(client, name, replication, durableWrites, options) {
    check.instanceStrict(client, cassandra.Client);
    check.nonEmptyString(name);
    // check.instanceStrict(replication, ReplicationStrategy);
    check.boolean(durableWrites);
    check.instanceStrict(options, KeyspaceOptions);

    this.client = client;
    this.name = name;
    this.replication = replication;
    this.durableWrites = durableWrites;
    this.options = options;
  }

  /**
   * Using the keyspace configuration settings, will enforce the keyspace exists
   * in the database.
   *
   * @param {Object} options
   * @return {Promise}
   */
  ensureExists(options) {
    options = _.extend({ alter: false }, this.options.ensureExists, options);
    
    // skip running
    if (!_.isUndefined(options.run) && !options.run) {
      ErrorHandler.logWarn(`Ensure keyspace skipped: ${this.name}.`);
      return Promise.resolve();
    }

    return new Promise((resolve, reject) => {
      this.selectSchema()
        .then(result => {
          if (!result || !result.rows) {
            reject(new errors.SelectSchemaError('Select schema returned no result or no rows.'));
          }
          else {
            if (result.rows.length === 0) {
              ErrorHandler.logWarn(`Creating keyspace: ${this.name}.`);
              
              this.create({ ifNotExists: true })
                .then(() => { resolve(); })
                .catch((err) => {
                  reject(new errors.CreateError(`Create keyspace failed: ${err}.`));
                });
            }
            // compare schema to existing keyspace
            else {
              const row = result.rows[0];
              let differentReplicationStrategy = false;

              if (row.replication.class !== this.replication.class) {
                differentReplicationStrategy = true;
              }
              else {
                _.each(this.replication, (value, key) => {
                  if (key !== 'class') {
                    if (_.isUndefined(row.replication[key])) {
                      differentReplicationStrategy = true;
                    }
                    else if (row.replication[key] !== value.toString()) { // values are stored as strings in schema
                      differentReplicationStrategy = true;
                    }
                  }
                });
              }
              
              // diff durable writes
              let differentDurableWrites = false;
              if (row.durable_writes !== this.durableWrites) {
                differentDurableWrites = true;
              }
              
              // log
              if (differentReplicationStrategy) {
                ErrorHandler.logWarn(`Different replication strategy found for existing keyspace: ${this.name}.`);
              }

              if (differentDurableWrites) {
                ErrorHandler.logWarn(`Different durable writes value found for existing keyspace: ${this.name}.`);
              }
              
              // fix
              if (options.alter && (differentReplicationStrategy || differentDurableWrites)) {
                ErrorHandler.logWarn('Altering keyspace to match schema...');
                
                this.alter(this.replication, this.durableWrites)
                  .then(() => { resolve(); })
                  .catch((err) => {
                    reject(new errors.FixError(`Alter keyspace failed: ${err}.`));
                  });
              }
              else {
                resolve();
              }
            }
          }
        })
        .catch(err => {
          reject(new errors.SelectSchemaError(`Error occurred trying to select schema: ${err}.`));
        });
    });
  }

  /**
   * Selects the keyspaces available on the system.
   *
   * @return {Promise}
   */
  selectSchema() {
    const query = {
      query: 'SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ? ALLOW FILTERING',
      params: [this.name],
      prepare: true
    };

    return this.execute(query);
  }

  /**
   * Creates a new keyspace in the Cassandra database.
   *
   * @param {Object} options
   * @return {Promise}
   */
  create(options) {
    options = _.extend({ ifNotExists: false }, options);
    
    const query = {
      query: 'CREATE KEYSPACE',
      params: [],
      prepare: true
    };
    
    if (options.ifNotExists) {
      query.query += ' IF NOT EXISTS';
    }
    
    this.concatBuilders([this.buildKeyspaceName, this.buildReplication, this.buildDurableWrites], query);
    
    return this.execute(query);
  }
  
  /**
   * Drops an existing keyspace from the Cassandra database.
   *
   * @param {Object} options
   * @return {Promise}
   */
  drop(options) {
    options = _.extend({ ifExists: false }, options);
    
    let query = {
      query: 'DROP KEYSPACE',
      params: [],
      prepare: true
    };
    
    if (options.ifExists) {
      query.query += ' IF EXISTS';
    }
    
    this.concatBuilders([this.buildKeyspaceName], query);
    
    return this.execute(query);
  }

  /**
   * Alters an existing keyspace from the Cassandra database.
   *
   * @param {Object} replication The replication class and parameters, see Cassandra documentation
   * @param {boolean} durableWrites Flag indicating if durable writes is set
   * @return {Promise}
   */
  alter(replication, durableWrites) {
    // check.instanceStrict(replication, ReplicationStrategy);
    check.boolean(durableWrites);

    const query = {
      query: 'ALTER KEYSPACE',
      params: [],
      prepare: true
    };
    
    this.concatBuilders.call(this, [this.buildKeyspaceName], query);
    
    let clause = '';
    if (!_.isNull(replication)) {
      clause += ` WITH REPLICATION = ${JSON.stringify(this.replication).replace(/"/g, "'")}`;
    }
    if (!_.isNull(durableWrites)) {
      if (clause.length > 0) {
        clause += ' AND';
      }
      else {
        clause += ' WITH';
      }
      clause += ` DURABLE_WRITES = ${durableWrites}`;
    }
    query.query += clause;
    
    return this.execute(query);
  }

  /**
   * Strings together the "builders" used to generate the query
   * string needed to run the command.
   *
   * @param {Array<Function>} builders An array of builders that generate a portion
   *                                   of the query string.
   * @param {Object} query The query object that holds the ends result
   * @param {string} query.query The resulting query string
   * @param {Array<*>} query.params The options parameters for the parameterized query string
   * @private
   */
  concatBuilders(builders, query) {
    _.each(builders, (builder) => {
      const result = builder.call(this);
      if (result.clause.length > 0) {
        query.query += ' ' + result.clause;
        query.params = query.params.concat(result.params);
      }
    });
  }

  /**
   * Builder for the keyspace name in the query.
   * @private
   */
  buildKeyspaceName() {
    let clause = this.name;
    let params = [];
    return { clause: clause, params: params };
  }

  /**
   * Builder for the replication setting in the query.
   * @private
   */
  buildReplication() {
    let clause = `WITH REPLICATION = ${JSON.stringify(this.replication).replace(/"/g, "'")}`;
    let params = [];
    return { clause: clause, params: params };
  }

  /**
   * Builder for the durable writes setting in the query.
   * @private
   */
  buildDurableWrites() {
    let clause = `AND DURABLE_WRITES = ${this.durableWrites}`;
    let params = [];
    return { clause: clause, params: params };
  }

  /**
   * Executes a given query and optionally parameterizes it if needed.
   *
   * @param {Object} query The query object that holds the ends result
   * @param {string} query.query The resulting query string
   * @param {Array<*>} query.params The options parameters for the parameterized query string
   * @return {Promise}
   * @private
   */
  execute(query) {
    ErrorHandler.logInfo(`Query: ${query.query}, Parameters: ${query.params}, Context: ${JSON.stringify({ prepare: query.prepare })}`);
    return new Promise((resolve, reject) => {
      this.client.execute(query.query, query.params, { prepare: query.prepare }, function(err, results) {
        if (err) reject(err);
        resolve(results);
      });
    });
  }
}
