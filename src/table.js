// Libraries
import Promise from 'bluebird';
import _ from 'lodash';
// Modules
import { errors, ErrorHandler } from './errors';
import * as helpers from './helpers';
import Schema from './schema';
import tableWithProperties from './table-with-properties';
import * as types from './types';
import Orm from './index';

/**
 * Cassandra-based table representation in the ORM.
 * @class
 */
export default class Table {
  constructor(orm, name, schema, options) {
    this.orm = orm;
    this.name = name;
    this.schema = schema;
    this.options = options;
  }

  ensureExists(options) {
    if (options && !helpers.isPlainObject(options)) {
      Promise.reject(new errors.Table.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeObject')));
    }
    
    // default options
    options = _.extend({ recreate: false, recreateColumn: false, removeExtra: false, addMissing: false }, this._options.ensureExists, options);
    
    // skip running
    if (!_.isUndefined(options.run) && !options.run) {
      callback();
      errorHandler.logWarn(i18n.t('warnings.orm.ensureTableSkipped', {keyspace: this._orm._keyspace, name: this._name}));
      return Promise.resolve();
    }

    console.log('Time To Find Table');
    return new Promise((resolve, reject) => {
      this.selectSchema()
        .then((result) => {
          console.log('Did Select Schema', result);
          if (!result || !result.rows) {
            reject(new errors.Table.SelectSchemaError(i18n.t('errors.orm.general.schemaNoRows')));
          }
          else {
            // create table
            if (result.rows.length === 0) {
              console.log('SS #1');
              errorHandler.logWarn(i18n.t('warnings.orm.creatingTable', {keyspace: this._orm._keyspace, name: this._name}));
              this.create({ ifNotExists: true })
                .then((result) => {
                  return resolve();
                })
                .catch((err) => {
                  return reject(new errors.Table.CreateError(i18n.t('errors.orm.general.failedToCreateTable', {err: err})));
                });
            }
            // compare schema to existing table
            else { 
              console.log('SS #123');
              // create hash for diff
              let columns = {};
              _.each(this._schema.columns(), (column, index) => {
                columns[column] = true;
              });
              
              // diff
              let mismatched = [];
              let extra = [];
              _.each(result.rows, (row, index) => {
                const column = row.column_name;
                if (columns[column]) {
                  console.log('SS #44');
                  const dbValidator = types.dbValidator(this._orm, this._schema.columnType(column));
                  console.log('SS #55');
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
              
              console.log('SS #165v');
              let missing = _.keys(columns);
              console.log('#44$212@');

              // log
              if (mismatched.length > 0) {
                console.log('#A');
                errorHandler.logWarn(
                  i18n.t('warnings.orm.mismatchedColumnType', {amount: mismatched.length, keyspace: this._orm._keyspace, name: this._name}),
                  mismatched
                );
              }

              if (extra.length > 0) {
                console.log('#B');
                errorHandler.logWarn(
                  i18n.t('warnings.orm.extraColumns', {amount: mismatched.length, keyspace: this._orm._keyspace, name: this._name}),
                  mismatched
                );
              }

              if (missing.length > 0) {
                console.log('#C');
                errorHandler.logWarn(
                  i18n.t('warnings.orm.missingColumns', {amount: mismatched.length, keyspace: this._orm._keyspace, name: this._name}),
                  mismatched
                );
              }
              
              // fix
              console.log('SS #3fv3');
              if ((mismatched.length > 0 || extra.length > 0 || missing.length > 0) && options.recreate) {
                recreate.call(this, callback);
              }
              else {
                let promises = [];
                if (mismatched.length > 0 && options.recreateColumn) {
                  promises = promises.concat(fixMismatched.call(this, _.map(mismatched, (mismatch, index) => { return mismatch.column; })));
                }

                if (extra.length > 0 && options.removeExtra) {
                  promises = promises.concat(fixExtra.call(this, extra));
                }

                if (missing.length > 0 && options.addMissing) {
                  promises = promises.concat(fixMissing.call(this, missing));
                }

                Promise.all(promises).then((descriptors) => {
                  resolve();
                }).catch((err) => {
                  reject(new errors.Table.FixError(i18n.t('errors.orm.general.failedToFixTable')));
                });
              }
            }
          }
        })
        .catch((err) => {
          console.log('WTF', err);
          reject(new errors.Table.SelectSchemaError(i18n.t('errors.orm.general.errSelectSchema', {err: err})));
        })
      });
  }

  recreate(callback) {
    return new Promise((resolve, reject) => {
      this.drop({ ifExists: true })
        .then((result) => {
          this.create({ ifNotExists: true })
            .then((result) => {
              resolve();
            })
            .catch((err) => {
              reject(new errors.Table.FixError(i18n.t('failedCreateTable', {err: err})));
            })
        })
        .catch((err) => {
          reject(new errors.Table.FixError(i18n.t('failedDropTable', {err: err})));
        })
    });
  }

  fixMismatched(mismatched) {
    errorHandler.logWarn(i18n.t('warnings.orm.recreatingColumns'));
  
    let promises = [];
    _.each(mismatched, (column, index) => {
      promises.push(
        new Promise((resolve, reject) => {
          this.dropColumn(column, (err, result) => {
            if (err) {
              reject(err);
            }
            else {
              this.addColumn(column, this._schema.columnType(column), (err, result) => {
                if (err) {
                  reject(err);
                }
                else {
                  resolve();
                }
              });
            }
          });
        })
      );
    });

    return promises;
  }

  fixExtra(extra) {
    errorHandler.logWarn(i18n.t('warnings.orm.removingExtraColumns'));
  
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

  fixMissing(missing) {
    errorHandler.logWarn(i18n.t('warnings.orm.addMissingColumns'));
    let promises = [];
    _.each(extra, (column, index) => {
      promises.push(
        new Promise((resolve, reject) => {
          this.addColumn(column, this._schema.columnType(column), (err, result) => {
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

  selectSchema() {
    let query = {
      query: 'SELECT * FROM system_schema.columns WHERE table_name = ? AND keyspace_name = ? ALLOW FILTERING',
      params: [this._name, this._orm._keyspace],
      prepare: true
    };

    console.log('Run Query', query);
    
    return new Promise((resolve) => {
      this._orm.execute(query.query, query.params, { prepare: query.prepare })
        .then((result) => {
          resolve(result);
        })
        .catch((err) => {
          reject(err);
        })
    });
  }

  create(options) {
    if (options && !helpers.isPlainObject(options)) {
      Promise.reject(new errors.Table.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeObject')));
    }
    console.log('SS #2');
    
    // default options
    options = _.extend({ ifNotExists: false }, options);
    
    let query = {
      query: 'CREATE TABLE',
      params: [],
      prepare: true
    };
    
    if (options.ifNotExists) {
      query.query += ' IF NOT EXISTS';
    }
    
    console.log('SS #2b');
    this.concatBuilders([this.buildTableName, this.buildColumns, this.buildWith], query);
    console.log('SS #3');

    return this._orm.execute(query.query, query.params, { prepare: query.prepare });
  }
  
  drop(options) {
    if (options && !helpers.isPlainObject(options)) {
      Promise.reject(new errors.Table.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeObject')));
    }
    
    // default options
    options = _.extend({ ifExists: false }, options);
    
    let query = {
      query: 'DROP TABLE',
      params: [],
      prepare: true
    };
    
    if (options.ifExists) {
      query.query += ' IF EXISTS';
    }
    
    this.concatBuilders([this.buildTableName], query);
    
    return this._orm.execute(query.query, query.params, { prepare: query.prepare });
  }

  addColumn(column, type) {
    if (!_.isString(column)) {
      Promise.reject(new errors.Table.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeString')));
    }
    else if (!_.isString(type)) {
      Promise.reject(new errors.Table.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeString')));
    }
    
    let query = {
      query: 'ALTER TABLE',
      params: [],
      prepare: true
    };
    
    this.concatBuilders([this.buildTableName], query);
    
    query.query += ' ADD "' + column + '" ' + type;
    
    return this._orm.execute(query.query, query.params, { prepare: query.prepare });
  }

  dropColumn(column) {
    if (!_.isString(column)) {
      Promise.reject(new errors.Table.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeString')));
    }
    
    let query = {
      query: 'ALTER TABLE',
      params: [],
      prepare: true
    };
    
    this.concatBuilders([this.buildTableName], query);
    
    query.query += ' DROP "' + column + '"';
    
    return this._orm.execute(query.query, query.params, { prepare: query.prepare });
  };

  renameColumn(column, newName) {
    if (!_.isString(column)) {
      Promise.reject(new errors.Table.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeString')));
    }
    else if (!_.isString(newName)) {
      Promise.reject(new errors.Table.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeString')));
    }
    
    let query = {
      query: 'ALTER TABLE',
      params: [],
      prepare: true
    };
    
    this.concatBuilders([this.buildTableName], query);
    
    query.query += ' RENAME "' + column + '" TO "' + newName + '"';
    
    return this._orm.execute(query.query, query.params, { prepare: query.prepare });
  }

  alterType(column, newType) {
    if (!_.isString(column)) {
      Promise.reject(new errors.Table.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeString')));
    }
    else if (!_.isString(newType)) {
      Promise.reject(new errors.Table.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeString')));
    }
    
    let query = {
      query: 'ALTER TABLE',
      params: [],
      prepare: true
    };
    
    this.concatBuilders([this.buildTableName], query);
    
    query.query += ' ALTER "' + column + '" TYPE ' + newType;
    
    return this._orm.execute(query.query, query.params, { prepare: query.prepare });
  }

  concatBuilders(builders, query) {
    _.each(builders, (builder) => {
      console.log('builder', builder);
      let result = builder.call(this);
      if (result.clause.length > 0) {
        query.query += ' ' + result.clause;
        query.params = query.params.concat(result.params);
      }
    });
  }

  buildTableName() {
    let clause = this._orm._keyspace + '.' + this._name;
    let params = [];
    return { clause: clause, params: params };
  }

  buildColumns() {
    let clause = '(';
    let params = [];
    
    // columns
    console.log('table #3');
    _.each(this._schema.columns(), (column, index) => {
      if (index > 0) {
        clause += ', ';
      }
      clause += '"' + column + '" ' + this._schema.columnType(column);
    });
    
    console.log('table #4');
    // key
    clause += ', PRIMARY KEY (';
    const partitionKey = this._schema.partitionKey();
    console.log('table #4k', partitionKey, this._schema.partitionKey());
    if (_.isArray(partitionKey)) {
      console.log('AA');
      clause += '(';
      _.each(partitionKey, (column, index) => {
        if (index > 0) {
          clause += ', ';
        }
        clause += '"' + column + '"';
      });
      clause += ')';
    }
    else {
      console.log('AA', partitionKey);
      clause += '"' + partitionKey + '"';
    }

    console.log('table #5', clause);
    const clusteringKey = this._schema.clusteringKey();
    console.log('table #77');
    if (clusteringKey) {
      clause += ', ';
      if (_.isArray(clusteringKey)) {
        console.log('table #6');
      _.each(clusteringKey, (column, index) => {
          if (index > 0) {
            clause += ', ';
          }
          clause += '"' + column + '"';
        });
      }
      else {
        clause += '"' + clusteringKey + '"';
      }
    }
    clause += ')';
    
    clause += ')';
    console.log('table #7');
    return { clause: clause, params: params };
  }

  buildWith() {
    let clause = '';
    let params = [];
    const properties = this._schema.with();
    if (properties) {
      clause += ' WITH';
      let i = 0;
      _.each(properties, (value, property) => {
        if (i > 0) {
          clause += ' AND';
        }
        if (property === '$clustering_order_by') {
          clause += ' ' + tableWithProperties.PROPERTIES[property] + '(';
          _.each(value, (order, column) => {
            clause += '"' + column + '" ' + tableWithProperties.CLUSTERING_ORDER[order];
          });
          clause += ')';
        }
        else if (property === '$compact_storage') {
          clause += ' ' + tableWithProperties.PROPERTIES[property];
        }
        else {
          throw new errors.Table.InvalidWith(i18n.t('errors.orm.general.InvalidWith'));
        }
        i++;
      });
    }
    return { clause: clause, params: params };
  }
}

export default Table;