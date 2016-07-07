// Libraries
import _ from 'lodash';
import Promise from 'bluebird';
import check from 'check-types';
// Modules
import { errors, ErrorHandler } from './errors';
import * as helpers from './helpers';
import Schema from './schema';
import tableWithProperties from './table-with-properties';
import * as types from './types';
import Orm from './index';

function checkOptions(options) {
  check.object(options) & check.boolean(options.recreate) & check.boolean(options.recreateColumn)
    & check.boolean(options.removeExtra) & check.boolean(options.addMissing) & check.boolean(options.ensureExists);
}

/**
 * Cassandra-based table representation in the ORM.
 * @class
 */
export default class Table {
  /**
   * @param {Orm} orm The instance of the ORM
   * @param {string} name The name of the table
   * @param {Object} schema The replication class and parameters, {@link ReplicationStrategy} 
   * @param {Object} [options] The options set for the table
   * @param {boolean} [options.recreate] Flag indicating whether or not to recreate the table implying
   *    a DROP and CREATE
   * @param {boolean} [options.recreateColumn] Flag indicating whether to just recreate each individual
   *    column in the table
   * @param {boolean} [options.removeExtra] Flag indicating whether to drop extra columns
   * @param {boolean} [options.addMissing] Flag indicating whether to add missing columns to the table
   * @param {boolean} [options.ensureExists] Flag indicating whether or not to run the creation/update of the
   *   table
   * @constructor
   */
  constructor(orm, name, schema, options) {
    check.instanceStrict(orm, Orm);
    check.nonEmptyString(name);
    checkOptions(options);

    this.orm = orm;
    this.name = name;
    this.schema = schema;
    this.options = options;
  }
 
  /**
   * Using the table configuration settings, will enforce the table meets the options specified
   * either creating the table, recreating it, or fixing the columns to match accordingly.
   *
   * @param {Object} [options] The options set for the table, see {@link Table#constructor}
   * @param {boolean} [options.recreate] Flag indicating whether or not to recreate the table implying
   *    a DROP and CREATE
   * @param {boolean} [options.recreateColumn] Flag indicating whether to just recreate each individual
   *    column in the table
   * @param {boolean} [options.removeExtra] Flag indicating whether to drop extra columns
   * @param {boolean} [options.addMissing] Flag indicating whether to add missing columns to the table
   * @param {boolean} [options.ensureExists] Flag indicating whether or not to run the creation/update of the
   *   table
   * @throws SelectSchemaError
   * @throws CreateError
   * @throws FixError
   * @public
   */
  ensureExists(options) {
    checkOptions(options);

    // default options
    options = _.extend({ recreate: false, recreateColumn: false, removeExtra: false, addMissing: false }, this.options, options);
    
    // skip running
    if (!_.isUndefined(options.ensureExists) && !options.ensureExists) {
      ErrorHandler.logWarn(`Ensure table skipped: ${this.orm.keyspace}.${this.name}.`);
      return Promise.resolve();
    }

    return new Promise((resolve, reject) => {
      this.selectSchema()
        .then(result => {
          if (!result || !result.rows) {
            return reject(new errors.SelectSchemaError('Select schema returned no result or no rows.'));
          }

          // create table
          if (result.rows.length === 0) {
            ErrorHandler.logWarn(`Creating table: ${this.orm.keyspace}.${this.name}.`);
            this.create({ ifNotExists: true })
              .then(result => {
                resolve();
              })
              .catch(err => {
                reject(new errors.CreateError(`Create table failed: ${err}.`));
              });
          }
          else { 
            let columns = {};
            _.each(this.schema.columns(), (column, index) => {
              columns[column] = true;
            });
            
            // diff
            let mismatched = [];
            let extra = [];
            _.each(result.rows, (row, index) => {
              const column = row.column_name;
              if (columns[column]) {
                const dbValidator = types.dbValidator(this.orm, this.schema.columnType(column));
                if (dbValidator !== row.validator) {
                  mismatched.push({
                    column: row.column_name,
                    expected: dbValidator,
                    actual: row.validator
                  });
                }
                delete columns[column];
              }
              else {
                extra.push(row.column_name);
              }
            });
            
            let missing = _.keys(columns);
            
            // log
            if (mismatched.length > 0) {
              ErrorHandler.logWarn(
                `Found ${mismatched.length} mismatched column types in ${this.orm.keyspace}.${this.name}`,
                mismatched
              );
            }

            if (extra.length > 0) {
              ErrorHandler.logWarn(
                `Found ${mismatched.length} extra columns in ${this.orm.keyspace}.${this.name}`,
                mismatched
              );
            }

            if (missing.length > 0) {
              ErrorHandler.logWarn(
                `Found ${mismatched.length} missing columns in ${this.orm.keyspace}.${this.name}`,
                mismatched
              );
            }
            
            // fix
            if ((mismatched.length > 0 || extra.length > 0 || missing.length > 0) && options.recreate) {
              this.recreate();
            }
            else {
              let promises = [];
              if (mismatched.length > 0 && options.recreateColumn) {
                promises = promises.concat(this.fixMismatched(_.map(mismatched, (mismatch, index) => { return mismatch.column; })));
              }

              if (extra.length > 0 && options.removeExtra) {
                promises = promises.concat(this.fixExtra(extra));
              }

              if (missing.length > 0 && options.addMissing) {
                promises = promises.concat(this.fixMissing(missing));
              }

              Promise.all(promises).then(descriptors => {
                resolve();
              }).catch(err => {
                reject(new errors.FixError(`Fixing table schema failed: rejected promises.`));
              });
            }
          }
        })
        .catch(err => {
          reject(new errors.SelectSchemaError(`Error occurred trying to select schema: ${err}.`));
        })
      });
  }

  /**
   * Recreates the entire table by DROPing it first and then
   * CREATEing it via the schema defined.
   *
   * @return {Promise} Resolves when the table has successfully been created, otherwise
   *  rejects with error
   * @throws FixError
   * @public
   */
  recreate() {
    return new Promise((resolve, reject) => {
      this.drop({ ifExists: true })
        .then(result => {
          this.create({ ifNotExists: true })
            .then(result => {
              resolve();
            })
            .catch(err => {
              reject(new errors.FixError(`Create table failed: ${err}.`));
            })
        })
        .catch(err => {
          reject(new errors.FixError(`Drop table failed: ${err}.`));
        })
    });
  }
  
  /**
   * Fixes any mismatched columns in the table by dropping the columns and
   * recreating them.
   *
   * @param {Array<string>} mismatched The set of mismatched columns
   * @return {Array<Promise>} Promises required that will resolve per each successful recreated column,
   *  otherwise reject with error
   * @private
   */
  fixMismatched(mismatched) {
    ErrorHandler.logWarn('Recreating columns with mismatched types...');
  
    let promises = [];
    _.each(mismatched, (column, index) => {
      promises.push(
        new Promise((resolve, reject) => {
          this.dropColumn(column, (err, result) => {
            if (err) {
              return reject(err);
            }
            
            this.addColumn(column, this.schema.columnType(column), (err, result) => {
              if (err) {
                reject(err);
              }
              else {
                resolve();
              }
            });
          });
        })
      );
    });

    return promises;
  }
  
  /**
   * Fixes any extra columns in the table by dropping them.
   *
   * @param {Array<string>} extra The set of extra columns
   * @return {Array<Promise>} Promises required that will resolve per each successful dropped column,
   *  otherwise reject with error
   * @private
   */
  fixExtra(extra) {
    ErrorHandler.logWarn('Removing extra columns...');
  
    let promises = [];
    _.each(extra, (column, index) => {
      promises.push(
        new Promise((resolve, reject) => {
          this.dropColumn(column, (err, result) => {
            if (err) {
              reject(err);
            }
            else {
              resolve();
            }
          });
        })
      );
    });

    return promises;
  }

  /**
   * Fixes any missing columns in the table by creating them.
   *
   * @param {Array<string>} missing The set of missing columns
   * @return {Array<Promise>} Promises required that will resolve per each successful created column,
   *  otherwise reject with error
   * @private
   */
  fixMissing(missing) {
    ErrorHandler.logWarn('Adding missing columns...');
    let promises = [];
    _.each(extra, (column, index) => {
      promises.push(
        new Promise((resolve, reject) => {
          this.addColumn(column, this.schema.columnType(column), (err, result) => {
            if (err) {
              reject(err);
            }
            else {
              resolve();
            }
          });
        })
      );
    });
    
    return promises;
  }

  /**
   * Selects the columns meta-data for the given table.
   *
   * @return {Promise} The results of the select columns schema
   * @public
   */
  selectSchema() {
    // As of 3.x system_schema is the keyspace we must specify
    let query = {
      query: 'SELECT * FROM system_schema.columns WHERE table_name = ? AND keyspace_name = ? ALLOW FILTERING',
      params: [this.name, this.orm.keyspace],
      prepare: true
    };
    
    return this.orm.execute(query.query, query.params, { prepare: query.prepare });
  }

  /**
   * Creates a new table based on the current table's schema.
   *
   * @param {Object} [options] The options for creating the table
   * @param {boolean} [options.ifNotExists] Flag indicating to use the IF NOT EXISTS clause
   *  for the query
   * @return {Promise} The results of the CREATE TABLE query
   * @public
   */
  create(options) {
    check.object(options) & check.boolean(options.ifNotExists);
    
    // default options
    options = _.extend({ ifNotExists: false }, options);
    
    let query = {
      query: 'CREATE TABLE',
      params: [],
      prepare: true
    };
    
    if (options.ifNotExists) {
      query.query = `${query.query} IF NOT EXISTS`;
    }
    
    this.concatBuilders([this.buildTableName, this.buildColumns, this.buildWith], query);

    return this.orm.execute(query.query, query.params, { prepare: query.prepare });
  }
  
  /**
   * Drops the current table.
   *
   * @param {Object} [options] The options for creating the table
   * @param {boolean} [options.ifExists] Flag indicating to use the IF EXISTS clause
   *  for the query
   * @return {Promise} The results of the DROP TABLE query
   * @public
   */
  drop(options) {
    check.object(options) & check.boolean(options.ifExists);

    // default options
    options = _.extend({ ifExists: false }, options);
    
    let query = {
      query: 'DROP TABLE',
      params: [],
      prepare: true
    };
    
    if (options.ifExists) {
      query.query = `${query.query} IF EXISTS`;
    }
    
    this.concatBuilders([this.buildTableName], query);
    
    return this.orm.execute(query.query, query.params, { prepare: query.prepare });
  }

  /**
   * Adds a new column to the current table.
   *
   * @param {string} column The name of the column
   * @param {string} type The columns data type
   * @return {Promise} The results of the ALTER TABLE query
   * @public
   */
  addColumn(column, type) {
    check.nonEmptyString(column);
    check.nonEmptyString(type);
    
    let query = {
      query: 'ALTER TABLE',
      params: [],
      prepare: true
    };
    
    this.concatBuilders([this.buildTableName], query);
    
    query.query = `${query.query} ADD "${column}" ${type}`;
    
    return this.orm.execute(query.query, query.params, { prepare: query.prepare });
  }

  /**
   * Drops a given column from the current table.
   *
   * @param {string} column The name of the column
   * @return {Promise} The results of the ALTER TABLE query
   * @public
   */
  dropColumn(column) {
    check.nonEmptyString(column);
    
    let query = {
      query: 'ALTER TABLE',
      params: [],
      prepare: true
    };
    
    this.concatBuilders([this.buildTableName], query);
    
    query.query = `${query.query} DROP "${column}"`;
    
    return this.orm.execute(query.query, query.params, { prepare: query.prepare });
  }

  /**
   * Renames a given column from the current table.
   *
   * @param {string} column The current name of the column
   * @param {string} newName The new name to give the column
   * @return {Promise} The results of the ALTER TABLE query
   * @public
   */
  renameColumn(column, newName) {
    check.nonEmptyString(column);
    check.nonEmptyString(newName);
    
    let query = {
      query: 'ALTER TABLE',
      params: [],
      prepare: true
    };
    
    this.concatBuilders([this.buildTableName], query);
    
    query.query = `${query.query} RENAME "${column}" TO "${newName}"`;
    
    return this.orm.execute(query.query, query.params, { prepare: query.prepare });
  }

  /**
   * Alerts the type for a given column from the current table.
   *
   * @param {string} column The name of the column
   * @param {string} newType The new data type to assign the column
   * @return {Promise} The results of the ALTER TABLE query
   * @public
   */
  alterType(column, newType) {
    check.nonEmptyString(column);
    check.nonEmptyString(newType);
    
    let query = {
      query: 'ALTER TABLE',
      params: [],
      prepare: true
    };
    
    this.concatBuilders([this.buildTableName], query);
    
    query.query = `${query.query} ALTER "${column}" TYPE ${newType}`;
    
    return this.orm.execute(query.query, query.params, { prepare: query.prepare });
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
      let result = builder.call(this);
      if (result.clause.length > 0) {
        query.query = `${query.query} ${result.clause}`;
        query.params = query.params.concat(result.params);
      }
    });
  }

  /**
   * Builder for the table's fully qualified name in the query.
   * @return {{clause: string, params: Array<*>}}
   * @private
   */
  buildTableName() {
    let clause = this.orm.keyspace + '.' + this.name;
    let params = [];
    return { clause: clause, params: params };
  }

  /**
   * Builder for the table's column information in the query.
   * @return {{clause: string, params: Array<*>}}
   * @private
   */
  buildColumns() {
    let clause = '(';
    let params = [];
    
    // columns
    _.each(this.schema.columns(), (column, index) => {
      if (index > 0) {
        clause = `${clause}, `;
      }
      clause = `${clause}"${column}" ${this.schema.columnType(column)}`;
    });
    
    // key
    clause = `${clause}, PRIMARY KEY (`;
    const partitionKey = this.schema.partitionKey();
    if (_.isArray(partitionKey)) {
      clause = `${clause}(`;
      _.each(partitionKey, (column, index) => {
        if (index > 0) {
          clause = `${clause}, `;
        }
        clause = `${clause}"${column}"`;
      });
      clause = `${clause})`;
    }
    else {
      clause = `${clause}"${partitionKey}"`;
    }

    const clusteringKey = this.schema.clusteringKey();
    if (clusteringKey) {
      clause = `${clause}, `;
      if (_.isArray(clusteringKey)) {
        _.each(clusteringKey, (column, index) => {
          if (index > 0) {
            clause = `${clause}, `;
          }
          clause = `${clause}"${column}"`;
        });
      }
      else {
        clause = `${clause}"${clusteringKey}"`;
      }
    }

    clause = `${clause})`;
    return { clause: clause, params: params };
  }
  
  /**
   * Builder for the table's WITH information in the query.
   * @return {{clause: string, params: Array<*>}}
   * @private
   * @throws InvalidWith
   * @see {@link https://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_table_r.html?scroll=reference_ds_v3f_vfk_xj__setting-a-table-property}
   */
  buildWith() {
    let clause = '';
    let params = [];
    const properties = this.schema.with();
    if (properties) {
      clause = `${clause} WITH`;
      let i = 0;
      _.each(properties, (value, property) => {
        if (i > 0) {
          clause = `${clause} AND`;
        }

        if (property === '$clustering_order_by') {
          clause = `${clause} ${tableWithProperties.PROPERTIES[property]}(`;
          _.each(value, (order, column) => {
            clause = `${clause}"${column}" ${tableWithProperties.CLUSTERING_ORDER[order]}`;
          });
          clause = `${clause})`;
        }
        else if (property === '$compact_storage') {
          clause = `${clause} ${tableWithProperties.PROPERTIES[property]}`;
        }
        else {
          throw new errors.InvalidWith(`Invalid with: ${property}.`);
        }
        i++;
      });
    }
    return { clause: clause, params: params };
  }
}
