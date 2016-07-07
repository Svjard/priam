// Libraries
import _ from 'lodash';
import cassandra from 'cassandra-driver';
// Modules
import { errors } from './errors';
import * as helpers from './helpers';
import types from './types';

const ACTIONS = {
  'select'   : 'SELECT',
  'update'   : 'UPDATE',
  'insert'   : 'INSERT',
  'delete'   : 'DELETE',
  'truncate' : 'TRUNCATE'
};

const WHERE_OPERATIONS = {
  '$eq' : '=',
  '$gt' : '=',
  '$gte': '>=',
  '$lt' : '<',
  '$lte': '<=',
  '$in' : 'IN'
};

const ORDERING = {
  '$asc' : 'ASC',
  '$desc' : 'DESC'
};

const USING = {
  '$ttl' : 'TTL',
  '$timestamp' : 'TIMESTAMP'
};

const UPDATE_OPERATIONS = {
  // all
  all: {
    '$set' : '%c = %v'
  },
  
  // sets
  set: {
    '$add'    : '%c = %c + %v',
    '$remove' : '%c = %c - %v'
  },
  
  // lists
  list: {
    '$prepend' : '%c = %v + %c',
    '$append' : '%c = %c + %v',
    '$remove' : '%c = %c - %v',
    '$inject' : '%c[%k] = %v'
  },
  
  map: {
    '$inject' : '%c[%k] = %v'
  },
  
  // counters
  counter: {
    '$incr' : '%c = %c + %v',
    '$decr' : '%c = %c - %v'
  }
};

const IF_OPERATIONS = WHERE_OPERATIONS;

class Query {
  constructor(model, instance) {
    this.model = model;
    this.instance = instance;
    
    // fields
    this.action = null;
    this.count = null;
    this.select = null;
    this.where = null;
    this.orderBy = null;
    this.limit = null;
    this.allowFiltering = null;
    this.using = null;
    this.ifExists = null;
    this.ifNotExists = null;
    this.insert = null
    this.update = null;
    this.if = null;
  }

  find(conditions) {
    this.where(conditions);
    return this.all();
  }

  findOne(conditions) {
    this.where(conditions);
    return this.first();
  }

  action(action) {
    this.action = action;
    return this;
  }

  select(columns) {
    _.each(columns, (column, index) => {
      if (_.isArray(column)) {
        if (column.length > 0) {
          if (!this.select) {
            this.select = {};
          }
          _.each(column, (c, index) => {
            this.select[c] = true;
          });
        }
      }
      else if (_.isString(column)) {
        if (!this.select) {
          this.select = {};
        }
        this.select[column] = true;
      }
    });

    return this;
  }

  where(arg1, arg2) {
    // normalize
    if (_.isString(arg1)) {
      const where = {};
      where[arg1] = { '$eq': arg2 };
      arg1 = where;
    }
    
    _.each(arg1, (conditions, column) => {  
      // normalize value
      if (!helpers.isPlainObject(conditions)) {
        conditions = { '$eq': conditions };
      }
      
      // add to conditions
      if (!this.where) {
        this.where = {};
      }
      if (!this.where[column]) {
        this.where[column] = {};
      }

      _.extend(this.where[column], conditions);
    });

    return this;
  };

  orderBy(arg1, arg2) {
    if (_.isString(arg1) && _.isString(arg2)) {
      const order = {};
      order[arg1] = arg2;
      arg1 = order;
    }
    
    _.each(arg1, (order, column) => {
      if (!ORDERING[order]) {
        throw new errors.InvalidOrderingError(i18n.t('errors.orm.general.invalidOrdering', { order: order }));
      }
    });

    this.orderBy = arg1;
    return this;
  }

  limit(limit) {
    this.limit = limit;
    return this;
  }

  allowFiltering(allow) {
    this.allowFiltering = allow;
    return this;
  }

  using(arg1, arg2) {
    // normalize
    if (_.isString(arg1) && helpers.isInteger(arg2)) {
      let using = {};
      using[arg1] = arg2;
      arg1 = using;
    }
    
    _.each(arg1, (value, using) => {
      if (!USING[using]) {
        throw new errors.InvalidUsingError(i18n.t('errors.orm.arguments.invalidUsing', { using: using }));
      }
    });

    if (!this.using) {
      this.using = {};
    }

    _.extend(this.using, arg1);

    return this;
  }

  ttl(ttl) {
    return this.using('$ttl', ttl);
  }

  timestamp(timestamp) {
    return this.using('$timestamp', timestamp);
  }

  ifExists(exists) {
    this.ifExists = exists;
    return this;
  }

  ifNotExists(notExists) {
    this.ifNotExists = notExists;
    return this;
  }

  update(arg1, arg2) {
    if (this.action && this.action !== 'update') {
      throw new errors.ActionConflictError(i18n.t('errors.orm.general.conflictingAction'));
    }
    else {
      this.action('update');
    }
    
    // normalize
    if (_.isString(arg1)) {
      let where = {};
      where[arg1] = { '$set': arg2 };
      arg1 = where;
    }
    
    _.each(arg1, (assignments, column) => {
      const type = this.model.schema.baseColumnType(column);
      
      // normalize value
      if (!helpers.isPlainObject(assignments)) {
        assignments = { '$set': assignments };
      }
      else if (type === 'map' || types.isUserDefinedType(this.model.orm, type)) {
        let containsOperators = false;
        _.each(assignments, (value, key) => {
          if (UPDATE_OPERATIONS.all[key] || UPDATE_OPERATIONS.map[key]) {
            containsOperators = true;
          }
        });

        if (!containsOperators) {
          assignments = { '$set': assignments };
        }
      }
      
      // validate operations
      _.each(assignments, (value, operator) => {
        if (!UPDATE_OPERATIONS.all[operator]) {
          if (!UPDATE_OPERATIONS[type] || !UPDATE_OPERATIONS[type][operator]) {
            throw new errors.InvalidUpdateOperationError(i18n.t('errors.orm.general.invalidOperator', { operator: operator }));
          }
        }
      });
      
      // add to conditions
      if (!this.update) {
        this.update = {};
      }
      
      // add injection assignments
      if (type === 'list' || type === 'map') {
        if (assignments['$inject']) {
          _.each(assignments['$inject'], (v, key) => {
            if (type === 'list') {
              this.update[column + '[' + key + ']'] = { '$set': v };
            }
            else {
              const mapKeyType = types.mapKeyType(this.model.orm, this.model.schema.columnType(column));
              if (types.isStringType(this.model.orm, mapKeyType)) {
                this.update[column + '[\'' + key + '\']'] = { '$set': v };
              }
              else {
                this.update[column + '[' + key + ']'] = { '$set': v };
              }
            }
          });
        }

        delete assignments['$inject'];
      }
      
      // add column level assignments
      if (!this.update[column]) {
        this.update[column] = {};
      }

      _.extend(this.update[column], assignments);
    });

    return this;
  }

  insert(arg1, arg2) {
    if (this.action && this.action !== 'insert') {
      throw new errors.ActionConflictError(i18n.t('errors.orm.general.conflictingAction'));
    }
    else {
      this.action('insert');
    }

    // normalize
    if (_.isString(arg1)) {
      let set = {};
      set[arg1] = arg2;
      arg1 = set;
    }
    
    if (!this.insert) {
      this.insert = {};
    }

    _.extend(this.insert, arg1);

    return this;
  }

  if(obj) {
    _.each(obj, (conditions, column) => {
      // normalize value
      if (_.isString(conditions)) {
        conditions = { '$eq': conditions };
      }
      
      // validate conditions
      if (!helpers.isPlainObject(conditions)) {
        throw new errors.InvalidTypeError(i18n.t('errors.orm.types.shouldBeObject'));
      }
      else {
        // validate operations
        _.each(conditions, (value, operator) => {
          if (!IF_OPERATIONS[operator]) {
            throw new errors.InvalidIfOperationError(i18n.t('errors.orm.general.invalidOperator', { operator: operator }));
          }
        });
        
        // add to conditions
        if (!this.if) {
          this.if = {};
        }

        if (!this.if[column]) {
          this.if[column] = {};
        }

        _.extend(this.if[column], conditions);
      }
    });

    return this;
  }

  execute() {
    const query = this.build();
    return this.model.execute(query.query, query.params, { prepare: query.prepare });
  };

  first() {
    if (this.action && this.action !== 'select') {
      Promise.reject(new errors.ActionConflictError(i18n.t('errors.orm.general.conflictingAction')));
    }
    else {
      this.action('select');
      this.limit(1);
      return new Promise((resolve, reject) => {
        this.execute()
          .then((result) => {
            console.log('at first #1', result);
            if (!_.isObject(result)) {
              reject(new errors.Query.UnexpectedType('Result type should be an object.'));
            }
            else if (!_.isArray(result.rows)) {
              reject(new errors.Query.UnexpectedType('Result.rows type should be an array.'));
            }
            else if (result.rows.length > 0 ){
              console.log('at first #2', result.rows[0]);
              resolve(this._model._newFromQueryRow(result.rows[0]));
            }
            else {
              resolve(null);
            }
          })
          .catch((err) => {
            reject(err);
          })
      });
    }
  }

  all() {
    console.log('AT ALL', this._action);
    if (this._action && this._action !== 'select') {
      console.log('AT ALL', 'FAILED');
      Promise.reject(new errors.Query.ActionConflict(i18n.t('errors.orm.general.conflictingAction')));
    }
    else {
      this.action('select');
      console.log('AT ALL', 'START');
      return new Promise((resolve, reject) => {
        console.log('AT ALL', 'RUN', this.execute);
        this.execute()
          .then((result) => {
            console.log('AT ALL', 'DONE', result);
            if (!_.isObject(result)) {
              reject(new errors.Query.UnexpectedType(i18n.t('errors.orm.results.shouldBeObject')));
            }
            else if (!_.isArray(result.rows)) {
              reject(new errors.Query.UnexpectedType(i18n.t('errors.orm.results.rowShouldBeArray')));
            }
            else {
              result = _.map(result.rows, (row, index) => {
                return this._model._newFromQueryRow(row);
              });
              resolve(result);
            }
          })
          .catch((err) => {
            console.log('AT ALL', 'DONE2', err);
            reject(err);
          })
      });
    }
  }

  count() {
    if (this._action && this._action !== 'select') {
      Promise.reject(new errors.Query.ActionConflit(i18n.t('errors.orm.general.conflictingAction')));
    }
    else {
      this._count = true;
      this.action('select');
      return new Promise((resolve, reject) => {
        this.execute()
          .then((result) => {
            if (!_.isObject(result)) {
              reject(new errors.Query.UnexpectedType(i18n.t('errors.orm.results.shouldBeObject')));
            }
            else if (!_.isArray(result.rows)) {
              reject(new errors.Query.UnexpectedType(i18n.t('errors.orm.results.rowShouldBeArray')));
            }
            else {
              result = result.rows[0];
              if (!_.isObject(result)) {
                reject(new errors.Query.UnexpectedType(i18n.t('errors.orm.results.shouldBeObject')));
              }
              else if (!_.isObject(result.count)) {
                reject(new errors.Query.UnexpectedType(i18n.t('errors.orm.results.shouldBeObject')));
              }
              else {
                resolve(result.count.toNumber());
              }
            }
          })
          .catch((err) => {
            reject(err);
          })
      });
    }
  }

  truncate() {
    this.action('truncate');
    return new Promise((resolve, reject) => {
      this.execute()
        .then((result) => {
          resolve(result);
        })
        .catch((err) => {
          reject(err);
        })
    });
  }

  deleteAll() {
    return this.truncate();
  }

  save(options) {
    if(this._where) {
      Promise.reject(new errors.Query.WhereConflict(i18n.t('errors.orm.general.invalidWhereSave')));
    }
    else if (!this._instance) {
      Promise.reject(new errors.Query.InstanceNotSet(i18n.t('errors.orm.general.modelRequired')));
    }
    
    console.log('this save', this._instance._save);
    return this._instance._save(this, options);
  }

  delete(options) {
    if (!_.isFunction(callback)) {
      throw new errors.Query.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeFunction'));
    }
    if (this._instance) {
      if(this._where) {
        throw new errors.Query.WhereConflict(i18n.t('errors.orm.general.invalidWhereDelete'));
      }
      this._instance._delete(callback, this, options);
    }
    else {
      if(!this._where) {
        throw new errors.Query.WhereNotSet(i18n.t('errors.orm.general.whereNotValidInDelete'));
      }
      this.action('delete').execute(callback);
    }
  }

  eachRow(modelCallback, completeCallback) {
    if (!_.isFunction(modelCallback) || !_.isFunction(completeCallback)) {
      throw new errors.Query.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeFunction'));
    }
    else {
      if (this._action && this._action !== 'select') {
        throw new errors.Query.ActionConflict(i18n.t('errors.orm.general.conflictingAction'));
      }
      else {
        this.action('select');
        
        const query = this.build();
        this._model._eachRow(query.query, query.params, { prepare: query.prepare }, (n, row) => {
          const result = this._model._newFromQueryRow(row);
          modelCallback(n, result);
        }, completeCallback);
      }
    }
  }

  stream() {
    if (this._action && this._action !== 'select') {
      throw new errors.Query.ActionConflict(i18n.t('errors.orm.general.conflictingAction'));
    }
    else {
      this.action('select');
      
      const query = this.build();
      return this._model._stream(query.query, query.params, { prepare: query.prepare });
    }
  }

  build() {
    if (!this._action || !ACTIONS[this._action]) {
      throw new errors.Query.InvalidAction(i18n.t('errors.orm.general.actionUnknown', {action: this._action}));
    }
    else {
      let query = {
        query: '',
        params: [],
        prepare: true
      };
      
      if (this._action === 'select') {
        this.concatBuilders([this.buildAction, this.buildSelectForSelectAction, this.buildFromForSelectAction, this.buildWhere, this.buildOrderBy, this.buildLimit, this.buildAllowFiltering], query);
      }
      else if (this._action === 'update') {
        this.concatBuilders([this.buildAction, this.buildFromForUpdateAction, this.buildUsing, this.buildUpdate, this.buildWhere, this.buildIf, this.buildIfExists], query);
      }
      else if (this._action === 'insert') {
        this.concatBuilders([this.buildAction, this.buildFromForInsertAction, this.buildInsert, this.buildIfNotExists, this.buildUsing], query);
      }
      else if (this._action === 'delete') {
        this.concatBuilders([this.buildAction, this.buildSelectForDeleteAction, this.buildFromForDeleteAction, this.buildUsing, this.buildWhere, this.buildIf, this.buildIfExists], query);
      }
      else if (this._action === 'truncate') {
        this.concatBuilders([this.buildAction, this.buildFromForTruncateAction], query);
      }
      
      return query;
    }
  }

  concatBuilders(builders, query) {
    _.each(builders, (builder) => {
      const result = builder.call(this);
      if (result.clause.length > 0) {
        query.query += ' ' + result.clause;
        query.params = query.params.concat(result.params);
      }
    });
  }

  buildAction() {
    let clause = ACTIONS[this._action];
    let params = [];
    return { clause: clause, params: params };
  }

  buildSelectForSelectAction() {
    let clause = '';
    let params = [];
    if (this._select) {
      _.each(this._select, (value, column) => {
        if (clause.length > 0) {
          clause += ', ';
        }
        clause += '"' + column + '"';
      });
    }

    if (clause.length === 0) {
      clause = '*';
    }

    if (!_.isNull(this._count) && this._count) {
      clause = 'COUNT(' + clause + ')'
    }

    return { clause: clause, params: params };
  }

  buildSelectForDeleteAction() {
    let clause = '';
    let params = [];
    if (this._select) {
      _.each(this._select, (value, column) => {
        if (clause.length > 0) {
          clause += ', ';
        }
        if (!this._model._schema.isColumn(column)) { // map[key] columns will return false
          clause += column;
        }
        else {
          clause += '"' + column + '"';
        }
      });
    }

    return { clause: clause, params: params };
  }

  buildFromForSelectAction() {
    let parts = this.buildFromForUpdateAction.apply(this);
    let params = parts.params;
    let clause = parts.clause;
    clause = 'FROM ' + clause;
    return { clause: clause, params: params };
  }

  buildFromForUpdateAction() {
    let clause = this._model._orm._keyspace + '.' + this._model._table._name;
    let params = [];
    return { clause: clause, params: params };
  }

  buildFromForInsertAction() {
    let parts = this.buildFromForUpdateAction.apply(this);
    let clause = parts.clause;
    let params = parts.params;
    clause = 'INTO ' + clause;
    return { clause: clause, params: params };
  }

  buildFromForDeleteAction() {
    return this.buildFromForSelectAction.apply(this);
  }

  buildFromForTruncateAction() {
    return this.buildFromForUpdateAction.apply(this);
  }

  buildWhere() {
    let clause = '';
    let params = [];
    if (this._where) {
      clause += 'WHERE';
      _.each(this._where, (conditions, column) => {
        _.each(conditions, (value, operator) => {
          if (params.length > 0) {
            clause += ' AND';
          }
          if (operator === '$in') {
            clause += ' "' + column + '" ' + WHERE_OPERATIONS[operator] + '(';
            _.each(value, (v, index) => {
              if (index > 0) {
                clause += ', ';
              }
              clause += '?';
              params.push(v);
            });
          }
          else {
            clause += ' "' + column + '" ' + WHERE_OPERATIONS[operator] + ' ?';
            params.push(value);
          }
        });
      });
    }
    return { clause: clause, params: params };
  }

  buildOrderBy() {
    let clause = '';
    let params = [];
    if (this._orderBy) {
      clause += 'ORDER BY';
      _.each(this._orderBy, (order, column) => {
        clause += ' "' + column + '" ' + ORDERING[order];
      });
    }
    return { clause: clause, params: params };
  }

  buildLimit() {
    let clause = '';
    let params = [];
    if (!_.isNull(this._limit)) {
      clause += 'LIMIT ' + this._limit;
    }
    return { clause: clause, params: params };
  }

  buildAllowFiltering() {
    let clause = '';
    let params = [];
    if (!_.isNull(this._allowFiltering) && this._allowFiltering) {
      clause += 'ALLOW FILTERING';
    }
    return { clause: clause, params: params };
  }

  buildUsing() {
    let clause = '';
    let params = [];
    if (this._using) {
      clause += 'USING';
      _.each(this._using, (value, using) => {
        if (params.length > 0) {
          clause += ' AND';
        }
        clause += ' ' + USING[using] + ' ?';
        params.push(value);
      });
    }
    return { clause: clause, params: params };
  }

  buildIfExists() {
    let clause = '';
    let params = [];
    if (!_.isNull(this._ifExists) && this._ifExists) {
      clause += 'IF EXISTS';
    }
    return { clause: clause, params: params };
  }

  buildIfNotExists() {
    let clause = '';
    let params = [];
    if (!_.isNull(this._ifNotExists) && this._ifNotExists) {
      clause += 'IF NOT EXISTS';
    }
    return { clause: clause, params: params };
  }

  buildUpdate() {
    let clause = '';
    let params = [];
    if (this._update) {
      clause += 'SET'
      _.each(this._update, (assignments, column) => {
        _.each(assignments, (value, operator) => {
          if (params.length > 0) {
            clause += ',';
          }
          
          let type = null;
          const match = column.match(/(\w+)\[.+\]/);
          if (match) {
            type = this._model._schema.baseColumnType(match[1]);
          }
          else {
            type = this._model._schema.baseColumnType(column);
          }
          
          let format = null;
          if (UPDATE_OPERATIONS.all[operator]) {
            format = UPDATE_OPERATIONS.all[operator];
          }
          else if (UPDATE_OPERATIONS[type] && UPDATE_OPERATIONS[type][operator]) {
            format = UPDATE_OPERATIONS[type][operator];
          }
          if (!format) {
            throw new errors.Query.InvalidUpdateOperation(i18n.t('errors.orm.general.invalidOperator', {operator: operator}));
          }
          else {
            let assignment = '';
            for(let i = 0; i < format.length; i++) {
              if (format[i] === '%') {
                if (format[i + 1] === 'c') {
                  
                  if (match) {
                    assignment += column; // don't quote injection assignments
                  }
                  else {
                    assignment += '"' + column + '"';
                  }
                  
                  i += 1;
                  continue;
                }
                else if (format[i + 1] === 'v') {
                  assignment += '?';
                  params.push(value);
                  i += 1;
                  continue;
                }
              }
              assignment += format[i];
            }
            clause += ' ' + assignment;
          }
        });
      });
    }
    return { clause: clause, params: params };
  }

  buildInsert() {
    let clause = '';
    let params = [];
    if (this._insert) {
      let columns = '(';
      let values = '(';
      _.each(this._insert, (value, column) => {
        if (columns.length > 1) {
          columns += ', ';
          values += ', ';
        }
        columns += '"' + column + '"';
        values += '?';
        params.push(value);
      });
      clause = columns + ') VALUES ' + values + ')';
    }
    return { clause: clause, params: params };
  }

  buildIf() {
    let clause = '';
    let params = [];
    if (this._if) {
      clause += 'IF';
      _.each(this._if, (conditions, column) => {
        _.each(conditions, (value, operator) => {
          if (params.length > 0) {
            clause += ' AND';
          }
          if (operator === '$in') {
            clause += ' "' + column + '" ' + IF_OPERATIONS[operator] + '(';
            _.each(value, (v, index) => {
              if (index > 0) {
                clause += ', ';
              }
              clause += '?';
              params.push(v);
            });
          }
          else {
            clause += ' "' + column + '" ' + IF_OPERATIONS[operator] + ' ?';
            params.push(value);
          }
        });
      });
    }
    return { clause: clause, params: params };
  }

  batch(client, queries, callback) {
    if (!(client instanceof cassandra.Client)) {
      throw new errors.Query.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeCassandra'));
    }
    else if (!_.isArray(queries)) {
      throw new errors.Query.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeArray'));
    }
    else if (!_.isFunction(callback)) {
      throw new errors.Query.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeFunction'));
    }
    else {
      // build queries
      for (let i = 0; i < queries.length; i++) {
        const query = queries[i];
        if (query instanceof Query) {
          queries[i] = query.build();
        }
      }
      
      client.batch(queries, { prepare: true }, (err) => {
        callback(err);
      });
    }
  }
};

export default Query;