// Libraries
import cassandra from 'cassandra-driver';
import check from 'check-types';
import _ from 'lodash';
// Modules
import { errors, ErrorHandler } from './errors';
import * as helpers from './helpers';
import { ReplicationStrategy } from './replication-strategies';

/**
 * Cassandra keyspace representation for the ORM.
 * @class
 */
export default class Keyspace {
  /**
   * @param {Client} client The instance of the Cassandra client
   * @param {string} name The name of the keyspace
   * @param {ReplicationStrategy} replication The replication class and parameters, {@link ReplicationStrategy} 
   * @param {boolean} durableWrites Flag indicating if durable writes is set
   * @param {Object} [options] The options set for the keyspace
   * @param {boolean} [options.ensureExists] Flag indicating whether or not to run the creation/update of the
   *   keyspace
   * @param {boolean} [options.alter] Flag indicating whether the keyspace should be altered if differences
   *   are found with the existing keyspace
   * @constructor
   */
  constructor(client, name, replication, durableWrites, options) {
    check.instanceStrict(client, cassandra.Client);
    check.nonEmptyString(name);
    check.instanceStrict(replication, ReplicationStrategy);
    check.boolean(durableWrites);
    check.object(options) & check.boolean(options.ensureExists) & check.boolean(options.alter);

    this.client = client;
    this.name = name;
    this.replication = replication;
    this.durableWrites = durableWrites;
    this.options = options || {};
  }

  /**
   * Using the keyspace configuration settings, will enforce the keyspace exists
   * in the database.
   *
   * @param {Object} options The options set for the keyspace, see {@link Keyspace#constructor}
   * @param {boolean} [options.ensureExists] Flag indicating whether or not to run the creation/update of the
   *   keyspace
   * @param {boolean} [options.alter] Flag indicating whether the keyspace should be altered if differences
   *   are found with the existing keyspace
   * @return {Promise} Will resolve if the keyspace is succesfully created or updated, otherwise will reject
   *                   with a specific error caught as listed in the 'thrown' errors
   * @throws SelectSchemaError
   * @throws CreateError
   * @throws FixError
   * @public
   */
  ensureExists(options) {
    check.object(options) & check.boolean(options.ensureExists) & check.boolean(options.alter);

    options = _.extend({ alter: false }, this.options, options);
    
    // skip running
    if (!_.isUndefined(options.run) && !options.run) {
      ErrorHandler.logWarn(`Ensure keyspace skipped: ${this.name}.`);
      return Promise.resolve();
    }

    return new Promise((resolve, reject) => {
      this.selectSchema()
        .then(result => {
          if (!result || !result.rows) {
            return reject(new errors.SelectSchemaError('Select schema returned no result or no rows.'));
          }

          if (result.rows.length === 0) {
            ErrorHandler.logWarn(`Creating keyspace: ${this.name}.`);
            
            this.create({ ifNotExists: true })
              .then(() => { resolve(); })
              .catch(err => {
                reject(new errors.CreateError(`Create keyspace failed: ${err}.`));
              });
          }
          // compare schema to existing keyspace
          else {
            const row = result.rows[0];
            let differentReplicationStrategy = false;

            if (!this.replication.equals(row.replication)) {
              differentReplicationStrategy = true;
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
                .catch(err => {
                  reject(new errors.FixError(`Alter keyspace failed: ${err}.`));
                });
            }
            else {
              resolve();
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
   * @return {Promise} The results of the select keyspace schema
   * @public
   */
  selectSchema() {
    // As of 3.x system_schema is the keyspace we must specify
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
   * @param {Object} options Specific options to building the query
   *                         to create the keyspace
   * @param {boolean} ifNotExists Flag indicating whether the "IF NOT EXISTS"
   *                              operator should be applied in the CREATE KEYSPACE statement
   * @return {Promise} The result of running the CREATE KEYSPACE statement
   * @public
   * {@link https://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_keyspace_r.html}
   */
  create(options) {
    options & check.object(options) & check.boolean(options.ifNotExists);

    options = _.extend({ ifNotExists: false }, options);
    
    const query = {
      query: 'CREATE KEYSPACE',
      params: [],
      prepare: true
    };
    
    if (options.ifNotExists) {
      query.query = `${query.query} IF NOT EXISTS`;
    }
    
    this.concatBuilders([this.buildKeyspaceName, this.buildReplication, this.buildDurableWrites], query);
    
    return this.execute(query);
  }
  
  /**
   * Drops an existing keyspace from the Cassandra database.
   *
   * @param {Object} options Specific options to building the query
   *                         to drop the keyspace
   * @param {boolean} ifExists Flag indicating whether the "IF EXISTS"
   *                           operator should be applied in the DROP KEYSPACE statement
   * @return {Promise} The result of running the DROP KEYSPACE statement
   * @public
   * {@link https://docs.datastax.com/en/cql/3.1/cql/cql_reference/drop_keyspace_r.html}
   */
  drop(options) {
    options & check.object(options) & check.boolean(options.ifExists);
    
    options = _.extend({ ifExists: false }, options);
    
    let query = {
      query: 'DROP KEYSPACE',
      params: [],
      prepare: true
    };
    
    if (options.ifExists) {
      query.query = `${query.query} IF EXISTS`;
    }
    
    this.concatBuilders([this.buildKeyspaceName], query);
    
    return this.execute(query);
  }

  /**
   * Alters an existing keyspace from the Cassandra database.
   *
   * @param {?ReplicationStrategy} replication The replication class and parameters, {@link ReplicationStrategy} 
   * @param {?boolean} durableWrites Flag indicating if durable writes is set
   * @return {Promise} The result of running the ALTER KEYSPACE statement
   * @public
   * {@link https://docs.datastax.com/en/cql/3.1/cql/cql_reference/alter_keyspace_r.html}
   */
  alter(replication, durableWrites) {
    check.instanceStrict(replication, ReplicationStrategy);
    check.boolean(durableWrites);

    const query = {
      query: 'ALTER KEYSPACE',
      params: [],
      prepare: true
    };
    
    this.concatBuilders.call(this, [this.buildKeyspaceName], query);
    
    let clause = '';
    if (!_.isNull(replication)) {
      clause = `${clause} WITH REPLICATION = ${JSON.stringify(this.replication.toCassandra()).replace(/"/g, "'")}`;
    }

    if (!_.isNull(durableWrites)) {
      if (clause.length > 0) {
        clause = `${clause} AND`;
      }
      else {
        clause = `${clause} WITH`;
      }
      clause = `${clause} DURABLE_WRITES = ${durableWrites}`;
    }
    query.query = `${query.query}${clause}`;
    
    return this.execute(query);
  }

  /**
   * Strings together the "builders" used to generate the query
   * string needed to run the command.
   *
   * @param {Array<function(): {{query: string, params: Array<*>}}>} builders An array of builders
   *  that generate a portion of the query string.
   * @param {Object} query The query object that holds the ends result
   * @param {string} query.query The resulting query string
   * @param {Array<*>} query.params The options parameters for the parameterized query string
   * @private
   */
  concatBuilders(builders, query) {
    _.each(builders, (builder) => {
      const result = builder.call(this);
      if (result.clause.length > 0) {
        query.query = `${query.query} ${result.clause}`;
        query.params = query.params.concat(result.params);
      }
    });
  }

  /**
   * Builder for the keyspace name in the query.
   * @return {{clause: string, params: Array<*>}}
   * @private
   */
  buildKeyspaceName() {
    let clause = this.name;
    let params = [];
    return { clause: clause, params: params };
  }

  /**
   * Builder for the replication setting in the query.
   * @return {{clause: string, params: Array<*>}}
   * @private
   */
  buildReplication() {
    let clause = `WITH REPLICATION = ${JSON.stringify(this.replication.toCassandra()).replace(/"/g, "'")}`;
    let params = [];
    return { clause: clause, params: params };
  }

  /**
   * Builder for the durable writes setting in the query.
   * @return {{clause: string, params: Array<*>}}
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
   * @param {Object} query The query object
   * @param {string} query.query The query string to be executed
   * @param {Array<*>} query.params The options parameters for the parameterized query string
   * @return {Promise} Resolves with the resultset if successfull, otherwise rejects with the client
   *                   specific error
   * @private
   * {@link http://docs.datastax.com/en/latest-nodejs-driver-api/Client.html}
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
