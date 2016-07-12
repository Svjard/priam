// Libraries
import _ from 'lodash';
// Modules
import { orm as errors } from './errors';
import * as helpers from './helpers';
import Orm from './index';
import Query from './query';
import Schema from './schema';
import Table from './table';
import * as types from './types';
import Validations from './validations';
import WrappedStream from './wrapped-stream';
import util from 'util';

const HOOKS = [
  'afterNew',
  'beforeCreate',
  'afterCreate',
  'beforeValidate',
  'afterValidate',
  'beforeSave',
  'afterSave',
  'beforeDelete',
  'afterDelete'
];

/**
 * Handler for validating a field in the model via
 * the validations.
 *
 * @param {string} column The name of the field in the model
 * @param {*} value The current value of the field
 * @param {Model} instance The instance of the model 
 * @return {(boolean | null)}
 * @ignore
 */
function validate(column, value, instance) {
  if (instance.validations) {
    const recipe = instance.validations.recipe(column);
    const displayName = displayNameFromRecipe(recipe, column);
    return Validations.validate(recipe, value, displayName, instance);
  }
  else {
    return null;
  }
}

function sanitize(column, value, instance) {
  if (instance._validations) {
    const recipe = instance.validations.recipe(column);
    return Validations.sanitize(recipe, value, instance);
  }
  else {
    return value;
  }
}

function validateSanitized(column, value, instance) {
  const recipe = instance._validations.recipe(column);
  const displayName = displayNameFromRecipe(recipe, column);
  return Validations.validateSanitized(recipe, value, displayName, instance);
}

function displayNameFromRecipe(recipe, column) {
  if (recipe.displayName) {
    return recipe.displayName;
  }
  else {
    return column;
  }
}

function set(column, value) {
  if (this.model.schema.isAlias(column)) {
    column = this.model.schema.columnFromAlias(column);
  }
  
  if (!this.model.schema.isColumn(column)) {
    throw new errors.InvalidColumnError(i18n.t('errors.orm.general.invalidColumn', { column: column }));
  }
  else if (this._model._schema.baseColumnType(column) === 'counter') {
    throw new errors.CannotSetCounterColumnsError(i18n.t('errors.orm.general.cannotSetCounterColumn', { column: column }));
  }
  
  console.log('set global #2');
  const prevValue = this.changes[column] ? this.changes[column].prev : this._get(column);
  
  // sanitize
  value = sanitize(column, value, this.model);
  
  // schema setter
  const setter = this._model._schema.columnSetter(column);
  if (setter) {
    value = setter.call(this, value);
  }
  
  console.log('set global #5');
  this._set(column, value);
  
  console.log('set global #6');

  // don't mark keys as changed for UPDATE operations
  if (this._upsert && this._model._schema.isKeyColumn(column)) {
    console.log('set global #7');
    return;
  }
  // mark column changed
  else {
    value = this._get(column);
    if (!helpers.isEqual(value, prevValue)) {
      this._changes[column] = { prev: prevValue, op: { '$set': true } };
    }
    else {
      delete this._changes[column];
    }
  }

  console.log('set global #8');
}

function _set(column, value) {
  if (!this._model._schema.isColumn(column)) {
    throw new errors.Model.InvalidColumn(i18n.t('errors.orm.general.invalidColumn', {column: column}));
  }
  
  console.log('global _set #1', column, value);

  // cassandra treats empty sets and lists as null values
  if (_.isArray(value) && value.length === 0) {
    value = null;
  }
  
  if (!_.isNull(value)) { // allow null values
    // cast string type to javascript types
    if (_.isString(value)) {
      const type = this._model._schema.baseColumnType(column);
      if (types.isNumberType(this._model._orm, type)) {
        value = parseFloat(value.replace(/[^\d\.\-]/g, ''));
        if (_.isNaN(value)) {
          value = null;
        }
      }
      else if (types.isBooleanType(this._model._orm, type)) {
        value = value !== '0' && value !== 'false' && value;
      }
    }
    // cast cassandra types to javascript types
    else {
      value = types.castValue(this._model._orm, value);
      console.log('global _set #2', column, value);
    }
    
    console.log('global _set #3', column, value);
    // validate type
    // recheck null, since casting can cast to null
    if (!_.isNull(value) && !this._model._schema.isValidValueTypeForColumn(column, value)) {
      throw new errors.Model.TypeMismatch(i18n.t('errors.orm.general.invalidColumnType', {column: column, type: this._model._schema.columnType(column)}));
    }

    console.log('global _set #4', column, value);
    
    // make set array uniq
    if (this._model._schema.baseColumnType(column) === 'set') {
      value = helpers.uniq(value);
    }

    console.log('global _set #5', column, value);
  }
  
  console.log('global _set #6', column, value);
  this._columns[column] = value;
}

function get(column) {
  if (!_.isString(column)) {
    throw new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeString'));
  }
  else if (!this._model._schema.isColumn(column)) {
    throw new errors.Model.InvalidColumn(i18n.t('errors.orm.general.invalidColumn', {column: column}));
  }
  
  if (this._upsert && !this._model._schema.isKeyColumn(column)) {
    if (!this._changes[column]) {
      throw new errors.Model.IndeterminateValue(i18n.t('errors.orm.general.readingUndefinedValue', {column: column}));
    }
    else if (!this._changes[column].op['$set']) {
      throw new errors.Model.IndeterminateValue(i18n.t('errors.orm.general.cannotReadIdentity', {column: column}));
    }
  }
  
  let value = this._get(column);
  
  // schema getter
  const getter = this._model._schema.columnGetter(column);
  if (getter) {
    value = getter.call(this, value);
  }
  
  return value;
}

function append(operation, column, value) {
  // if update, only record idempotent operations
  if (this._upsert) {
    if (this._changes[column]) {
      if (this._changes[column].op[operation]) {
        this._changes[column].op[operation].push(value);
      }
      else {
        throw new lErrors.Model.OperationConflict(i18n.t('errors.orm.general.multipleConflictingOps', {column: column}));
      }
    }
    else {
      let op = {};
      op[operation] = [value];
      this._changes[column] = { op: op };
    }
    return;
  }
  
  // full set and change tracking
  const prevValue = this._changes[column] ? this._changes[column].prev : this._get(column);
  
  // append
  let newValue = this._get(column);
  if (newValue) {
    newValue = newValue.concat([value]); // use concat to return a copy
  }
  else {
    newValue = [value];
  }
  
  this._typeSpecificSet.call(this, column, newValue);
  
  // mark column changed
  newValue = this._get(column);
  if (!lHelpers.isEqual(newValue, prevValue)) {
    if (this._changes[column]) {
      if (this._changes[column].op[operation]) {
        this._changes[column].op[operation].push(value);
      }
      else {
        this._changes[column].op = { '$set': true };
      }
    }
    else {
      let op = {};
      op[operation] = [value];
      this._changes[column] = { prev: prevValue, op: op };
    }
  }
  else {
    delete this._changes[column];
  }
}

function ensureTable(CustomModel) {
  console.log('ensure table at', CustomModel._options.tableName(CustomModel._name));
  CustomModel._table = new Table(CustomModel._orm, CustomModel._options.tableName(CustomModel._name), CustomModel._schema, CustomModel._options.table);
  return new Promise((resolve, reject) => {
    CustomModel._table.ensureExists()
      .then(() => {
        console.log('ensure Table #1');
        resolve();
      })
      .catch((err) => {
        console.log('ensure Table #2', err);
        reject(new errors.Model.EnsureTableExists(i18n.t('errors.orm.general.failedEnsuringTable', { err: err })));
      });
  });
}

export default class Model {
  /**
   * Base class for defining models for the ORM.
   * @param {Object<string, *>} [attrs] The initial set of attributes
   * @param {!Object} [options] The model options
   * @class Model
   */
  constructor(attrs, options) {
    /* type-check */
    if (attrs) {
      check.assert.object(attrs);
    }

    if (options) {
      check.assert.object(options);
    }
    /* end-type-check */
    this.exists = false;
    this.upsert = false;
    this.columns = {};
    this.changes = {};
    this.prevChanges = {};
    this.invalidColumns = null;
    this.ready = false;
    this.queryQueue = {
      execute: [],
      eachRow: [],
      stream: []
    };

    this.orm = orm;
    this.name = name || new this().constructor.name;
    this.options = options;

    // set the initial set of attributes on the model
    this.set(attrs);

    if (!options || !options.skipAfterNewCallback) {
      this.afterNew();
    }
  }
  
  /**
   * Validates the fields in the model based on the validations.
   *
   * @param {!Object} [options] Set of options to apply in validating
   * @param {boolean} [options.only] 
   * @param {boolean} [options.except] 
   * @return {Promise}
   * @private
   * @function columns
   * @memberOf Model
   * @instance
   */
  validate(options) {
    /* type-check */
    if (options) {
      check.assert.object(options) & check.assert.boolean(options.only) & check.assert.boolean(options.except);
    }
    /* end-type-check */
    options = _.extend({}, options);
    
    if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipBeforeValidate)) {
      this.beforeValidate();
    }
    
    let columns = null;
    // if update, only validate changed columns that aren't idempotent operations
    if (this.upsert) {
      columns = _.reduce(_.keys(this.changes), (memo, column) => {
        if (this.changes[column].op['$set']) {
          memo.push(column);
        }
        return memo;
      }, []);
    } else {
      columns = this.schema().columns();
    }
    
    let invalidColumns = null;
    _.each(columns, (column, index) => {
      if (!options || !(options.only || options.except) || (options.only && options.only.indexOf(column) > -1) || (options.except && options.except.indexOf(column) === -1)) {
        const messages = this.model.validate(column, this.get(column), this); // TODO -- ??
        if (messages) {
          if (!invalidColumns) {
            invalidColumns = {};
          }
          invalidColumns[column] = messages;
        }
      }
    });

    this.invalidColumns = invalidColumns;
    
    if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipAfterValidate)) {
      this.afterValidate();
    }
    
    return invalidColumns;
  }

  /**
   * Gets the current set of invalid columns, i.e. those which failed validation
   *
   * @return {Array<string>}
   * @public
   * @function invalidColumnsfunction
   * @memberOf Model
   * @instance
   */
  invalidColumnsfunction() {
    return this.invalidColumns;
  }

  /**
   * Sets the value of a column.
   *
   * @param {string|Object<String, *>} column The name of the column or map of columns
   *  and values to be set
   * @param {*} [value] The value to set the value to
   * @public
   * @function set
   * @memberOf Model
   * @instance
   */
  set(column, value) {
    /* type-check */
    column.assert.nonEmptyString(column);
    /* end-type-check */

    if (helpers.isPlainObject(column)) {
      _.each(column, (v, c) => {
        this.set(c, v);
      });
    }
    else if (_.isString(column)) {
      this.set(column, value);
    }
  }

  _set(column, value) {
    // disallow setting primary key columns
    if (!this._upsert && this._exists && this._model._schema.isKeyColumn()) {
      throw new errors.Model.CannotSetKeyColumns(i18n.t('errors.orm.general.primaryKeyIsImmutable', {column: column}));
    }
    
    if (_.isObject(column)) {
      _.each(column, (v, c) => {
        console.log('_set #1', c, v);
        _set.call(this, c, v);
      });
    }
    else if (_.isString(column)) {
      console.log('_set #2', column, value);
      _set.call(this, column, value);
    }
    else {
      throw new errors.Model.InvalidArgument('Type should be a string.');
    }
  }
 
  /**
   * Get the value of a column.
   *
   * @param {string|Object<String, *>} column The name of the column or array of
   *  columns to fetch the value for
   * @return {*} The value of the column or an Map of column name to values if 
   *  multiple columns were specified
   * @public
   * @function get
   * @memberOf Model
   * @instance
   */
  get(column) {
    if (_.isArray(column)) {
      let columns = {};
      _.each(column, (c, index) => {
        columns[c] = this.get(c);
      });
      return columns;
    }
    else if (_.isString(column)) {
      return get.call(this, column);
    }
    else {
      throw new errors.InvalidArgument('Column name should be a string');
    }
  }

  _get(column) {
    if (!_.isString(column)) {
      throw new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeString'));
    }
    else if (!this._model._schema.isColumn(column)) {
      throw new errors.Model.InvalidColumn(i18n.t('errors.orm.general.invalidColumn', {column: column}));
    }
    
    let value = this._columns[column];
    
    // cast undefined
    if (_.isUndefined(value)) {
      // cast counters to 0
      if (this._model._schema.columnType(column) === 'counter') {
        value = 0;
      }
      // cast everything else to null
      else {
        value = null;
      }
    }
    
    return value;
  }

  /**
   * Adds a new value to a set type for a given column.
   *
   * @param {string} column The name of the column
   * @param {*} value The value to append to the set
   * @public
   * @function add
   * @memberOf Model
   * @instance
   */
  add(column, value) {
    /* type-check */
    check.assert.nonEmtpyString(column);
    /* end-type-check */
    if (!this.model.schema.isColumn(column)) {
      throw new errors.InvalidColumn(`Invalid column: ${column}.`);
    }
    else if (this.model.schema.baseColumnType(column) !== 'set') {
      throw new errors.InvalidColumnType('Add can only be performed on columns of type set.');
    }
    
    // don't allow adding duplicates
    const newValue = this.get(column);
    if (newValue && newValue.indexOf(value) > -1) {
      return;
    }
    
    this.append('$add', column, value);
  }
  
  /**
   * Removes a value from a collection for a given column.
   *
   * @param {string} column The name of the column
   * @param {*} value The value to remove from the collection
   * @public
   * @function remove
   * @memberOf Model
   * @instance
   */
  remove(column, value) {
    /* type-check */
    check.assert.nonEmtpyString(column);
    /* end-type-check */
    if (!this.model.schema.isColumn(column)) {
      throw new errors.InvalidColumn(`Invalid column: ${column}.`);
    }
    else {
      const baseType = this.model.schema.baseColumnType(column)
      if (baseType !== 'set' && baseType !== 'list' && baseType !== 'map') {
        throw new errors.InvalidColumnType('Remove can only be performed on columns of type set, list, and map.');
      }
    }
    
    // if map, inject a null value
    if (baseType === 'map') {
      this.inject(column, value, null);
      return;
    }
    
    // if update, only record idempotent operations
    if (this.upsert) {
      if (this.changes[column]) {
        if (this.changes[column].op['$remove']) {
          this.changes[column].op['$remove'].push(value);
        }
        else {
          throw new errors.OperationConflict(`Multiple conflicting operations on column: ${column}.`);
        }
      }
      else {
        this.changes[column] = { op: { '$remove': [value] } };
      }
      return;
    }
    
    // full set and change tracking
    const prevValue = this.changes[column] ? this.changes[column].prev : this.get(column);
    let newValue = null;
    
    // remove
    newValue = this.get(column);
    if (newValue) {
      newValue = helpers.without(newValue, value);
      if (newValue.length === 0) {
        newValue = null;
      }
    }
    else {
      newValue = null;
    }
    
    this.typeSpecificSet(column, newValue);
    
    // mark column changed
    newValue = this.get(column);
    if (!helpers.isEqual(newValue, prevValue)) {
      if (this.changes[column]) {
        if (this.changes[column].op['$remove']) {
          this.changes[column].op['$remove'].push(value);
        }
        else {
          this.changes[column].op = { '$set': true };
        }
      }
      else {
        this.changes[column] = { prev: prevValue, op: { '$remove': [value] } };
      }
    }
    else {
      delete this.changes[column];
    }
  }

  prepend(column, value) {
    if (!this._model._schema.isColumn(column)) {
      throw new errors.Model.InvalidColumn(i18n.t('errors.orm.general.invalidColumn', {column: column}));
    }
    else if (this._model._schema.baseColumnType(column) !== 'list') {
      throw new errors.Model.InvalidColumnType(i18n.t('errors.orm.general.prependOnlyOnList'));
    }
    
    // if update, only record idempotent operations
    if (this._upsert) {
      if (this._changes[column]) {
        if (this._changes[column].op['$prepend']) {
          this._changes[column].op['$prepend'].push(value);
        }
        else {
          throw new errors.Model.OperationConflict(i18n.t('errors.orm.general.multipleConflictingOps', {column: column}));
        }
      }
      else {
        this._changes[column] = { op: { '$prepend' : [value] } };
      }
      return;
    }
    
    // full set and change tracking
    const prevValue = this._changes[column] ? this._changes[column].prev : this._get(column);
    
    // prepend
    let newValue = this._get(column);
    if (newValue) {
      newValue = [value].concat(newValue); // use concat to return a copy
    }
    else {
      newValue = [value];
    }
    
    this._typeSpecificSet.call(this, column, newValue);
    
    // mark column changed
    newValue = this._get(column);
    if (!lHelpers.isEqual(newValue, prevValue)) {
      if (this._changes[column]) {
        if (this._changes[column].op['$prepend']) {
          this._changes[column].op['$prepend'].push(value);
        }
        else {
          this._changes[column].op = { '$set': true };
        }
      }
      else {
        this._changes[column] = { prev: prevValue, op: { '$prepend' : [value] } };
      }
    }
    else {
      delete this._changes[column];
    }
  }

  append(column, value) {
    if (!this._model._schema.isColumn(column)) {
      throw new errors.Model.InvalidColumn(i18n.t('errors.orm.general.invalidColumn', {column: column}));
    }
    else if (this._model._schema.baseColumnType(column) !== 'list') {
      throw new errors.Model.InvalidColumnType(i18n.t('errors.orm.general.appendOnlyOnList'));
    }
    
    append.call(this, '$append', column, value);
  }

  inject(column, key, value) {
    if (!this._model._schema.isColumn(column)) {
      throw new errors.Model.InvalidColumn(i18n.t('errors.orm.general.invalidColumn', {column: column}));
    }
    else {
      const type = this._model._schema.baseColumnType(column);
      if (type !== 'list' && type !== 'map') {
        throw new errors.Model.InvalidColumnType(i18n.t('errors.orm.general.injectOnlyOnListOrMap'));
      }
      else if (type === 'list' && !helpers.isInteger(key)) {
        throw new errors.Model.InvalidArgument(i18n.t('errors.orm.general.keyMustBeInt'));
      }
    }
    
    // if update, only record idempotent operations
    if (this._upsert) {
      if (this._changes[column]) {
        if (this._changes[column].op['$inject']) {
          this._changes[column].op['$inject'][key] = value;
        }
        else {
          throw new errors.Model.OperationConflict(i18n.t('errors.orm.general.multipleConflictingOps', {column: column}));
        }
      }
      else {
        let op = {};
        op['$inject'] = {};
        op['$inject'][key] = value;
        this._changes[column] = { op: op };
      }
      return;
    }
    
    // full set and change tracking
    const prevValue = this._changes[column] ? this._changes[column].prev : this._get(column);
    
    // create empty object if null
    let newValue = this._get(column);
    if (!newValue) {
      if (!_.isNull(value)) {
        if (type === 'list') {
          newValue = [];
        }
        else {
          newValue = {};
        }
      }
    }
    // shallow copy
    else {
      const currentValue = newValue;
      if (_.isArray(newValue)) {
        newValue = [];
      }
      else {
        newValue = {};
      }

      _.each(currentValue, (value, key) => {
        newValue[key] = value;
      });
    }
    
    // validate index
    if (type === 'list' && (!newValue || key >= newValue.length)) {
      throw new errors.Model.InvalidArgument(i18n.t('errors.orm.general.outOfBounds', {column: column }));
    }
    
    // inject
    if (_.isNull(value)) {
      if (newValue) {
        if (type === 'list') {
          newValue.splice(key, 1);
        }
        else {
          delete newValue[key];
        }

        if (_.size(newValue) === 0) {
          newValue = null;
        }
      }
    }
    else {
      newValue[key] = value;
    }
    
    this._typeSpecificSet.call(this, column, newValue);
    
    // mark column changed
    newValue = this._get(column);
    if (!helpers.isEqual(newValue, prevValue)) {
      if (this._changes[column]) {
        if (this._changes[column].op['$inject']) {
          this._changes[column].op['$inject'][key] = value;
        }
        else {
          this._changes[column].op = { '$set': true };
        }
      }
      else {
        let op = {};
        op['$inject'] = {};
        op['$inject'][key] = value;
        this._changes[column] = { prev: prevValue, op: op };
      }
    }
    else {
      delete this._changes[column];
    }
  }

  increment(column, delta) {
    if (!this._model._schema.isColumn(column)) {
      throw new errors.Model.InvalidColumn(i18n.t('errors.orm.general.invalidColumn', {column: column}));
    }
    else if (this._model._schema.baseColumnType(column) !== 'counter') {
      throw new errors.Model.InvalidColumnType(i18n.t('errors.orm.general.counterColumnOp', {op: 'Increment'}));
    }
    
    this._increment(column, delta);
  }

  decrement(column, delta) {
    if (!this._model._schema.isColumn(column)) {
      throw new errors.Model.InvalidColumn(i18n.t('errors.orm.general.invalidColumn', {column: column}));
    }
    else if (this._model._schema.baseColumnType(column) !== 'counter') {
      throw new errors.Model.InvalidColumnType(i18n.t('errors.orm.general.counterColumnOp', {op: 'Decrement'}));
    }
    
    this._increment(column, -delta);
  }

  _increment(column, delta) {
    // if update, only record idempotent operations
    if (this._upsert) {
      if (this._changes[column]) {
        delta += this._changes[column].op['$incr'] ? this._changes[column].op['$incr'] : this._changes[column].op['$decr'];
      }
      if (delta > 0) {
        this._changes[column] = { prev: prevValue, op: { '$incr': delta } };
      }
      else if (delta < 0) {
        this._changes[column] = { prev: prevValue, op: { '$decr': -delta } };
      }
      else {
        delete this._changes[column];
      }
      return;
    }
    
    // full set and change tracking
    const prevValue = this._changes[column] ? this._changes[column].prev : this._get(column);
    
    // increment
    let newValue = this._get(column) + delta;
    
    this._typeSpecificSet.call(this, column, newValue);
    
    // mark column changed
    newValue = this._get(column);
    if (newValue !== prevValue) {
      delta = newValue - prevValue;
      let op = {};
      if (delta > 0) {
        op['$incr'] = delta;
      }
      else {
        op['$decr'] = -delta;
      }
      this._changes[column] = { prev: prevValue, op: op };
    }
    else {
      delete this._changes[column];
    }
  }

  _typeSpecificSet(column, value) {
    // sanitize
    value = this._model.sanitize(column, value);
    
    // schema setter
    const setter = this._model._schema.columnSetter(column);
    if (setter) {
      value = setter.call(this, value);
    }
    
    this._set(column, value);
  }

  changed(column) {
    return this._changed(this._changes, column);
  }

  prevChanged(column) {
    return this._changed(this._prevChanges, column);
  }

  _changed(changes, column) {
    if (column && !_.isString(column)) {
      throw new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeString'));
    }
    else if (column && !this._model._schema.isColumn(column)) {
      throw new errors.Model.InvalidColumn(i18n.t('errors.orm.general.invalidColumn', {column: column}));
    }
    else {
      if (column) {
        return !!changes[column];
      }
      else {
        return _.size(changes) > 0;
      }
    }
  }

  changes(column) {
    return this._changes.call(this, this._changes, column);
  }

  prevChanges(column) {
    return this._changes.call(this, this._prevChanges, column);
  }

  _changes(changes, column) {
    if (column && !_.isString(column)) {
      throw new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeString'));
    }
    else if (column && !this._model._schema.isColumn(column)) {
      throw new errors.Model.InvalidColumn(i18n.t('errors.orm.general.invalidColumn', {column: column}));
    }
    else {
      if (column) {
        const value = changes[column];
        if (!_.isUndefined(value)) {
          const newValue = this._get(column);
          return { from: value.prev, to: newValue, op: !value.op['$set'] ? value.op : { '$set' : newValue } }
        }
        else {
          return value;
        }
      }
      else {
        let c = {};
        _.each(changes, (value, column) => {
          const newValue = this._get(column);
          c[column] = { from: value.prev, to: newValue, op: !value.op['$set'] ? value.op : { '$set' : newValue } }
        });
        return c;
      }
    }
  }

  /**
   * Generates a ORM model which has compatible schema and validation set.
   *
   * @return {Promise}
   * @private
   * @function columns
   * @memberOf Model
   * @static
   */
  static compile(orm, name, schema, validations, options) {
    return new Promise((resolve, reject) => {
      ensureTable(this)
        .then(() => {
          this.ready = true;
          this.processQueryQueue();
          resolve();
        });
    });
  }

  static validate(column, value, instance) {
    if (_.isString(column)) {
      return validate(column, value, instance); // call with this context
    }
    else if (helpers.isPlainObject(column)) {
      instance = value;
      value = null;
      let invalidColumns = null;
      _.each(column, function(v, c) {
        const messages = validate(c, v, instance);
        if (messages) {
          if (!invalidColumns) {
            invalidColumns = {};
          }
          invalidColumns[c] = messages;
        }
      });
      return invalidColumns;
    }
    else {
      throw new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeStringOrObject'));
    }
  }

  static sanitize(column, value, instance) {
    if (_.isString(column)) {
      return sanitize(column, value, instance);
    }
    else if (helpers.isPlainObject(column)) {
      instance = value;
      value = null;
      let values = {};
      _.each(column, (v, c) => {
        values[c] = sanitize(c, v, instance);
      });
      return values;
    }
    else {
      throw new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeStringOrObject'));
    }
  }

  static validateSanitized(column, value, instance) {
    if (_.isString(column)) {
      return validateSanitized(column, value, instance);
    }
    else if (helpers.isPlainObject(column)) {
      instance = value;
      value = null;
      let invalidColumns = null;
      _.each(column, (v, c) => {
        const messages = validateSanitized(c, v, instance);
        if (messages) {
          if (!invalidColumns) {
            invalidColumns = {};
          }
          invalidColumns[c] = messages;
        }
      });
      return invalidColumns;
    }
    else {
      throw new lErrors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeStringOrObject'));
    }
  }

  static new(assignments) {
    return new this(assignments);
  }

  static create(assignments) {
    if (!_.isFunction(assignments)) {
      if (!helpers.isPlainObject(assignments)) {
        Promise.reject(new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeObject')));
      }
    }
    else {
      callback = assignments;
      assignments = {};
    }

    console.log('create', assignments);
    
    console.log('test this', this);
    let model = new this(assignments);
    console.log('create ##1');
    return new Promise((resolve) => {
      console.log('create ##2');
      model.save((err, model) => {
        if (err) return reject(err);
        resolve(model);
      })
    });
  }

  static _newFromQueryRow(row) {
    let model = new this({}, { _skipAfterNewCallback: true });
    model._set(row);
    model._exists = true; // ordering matters for key setting
    return model;
  }

  static upsert(assignments, callback) {
    if (assignments && !helpers.isPlainObject(assignments)) {
      throw new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeObject'));
    }
    else if (callback && !_.isFunction(callback)) {
      throw new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeFunction'));
    }
    
    let model = new this({}, { _skipAfterNewCallback: true });
    model._upsert = true; // ordering matters for key setting
    model._exists = true; // ordering matters for key setting
    if (assignments) {
      model.set(assignments);
    }

    if (callback) {
      model.save(callback);
    }

    return model;
  }

  static delete(where, callback) {
    if (!helpers.isPlainObject(where)) {
      throw new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeObject'));
    }
    else if (_.size(where) === 0) {
      throw new errors.Model.InvalidArgument('Delete where condition cannot be empty. Run deleteAll or truncate to remove all records.');
    }
    else if (!_.isFunction(callback)) {
      throw new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeFunction'));
    }
    
    this.where(where).delete(callback);
  }

  static find(conditions) {
    console.log('find', 'conditions', conditions);
    if (conditions && !helpers.isPlainObject(conditions)) {
      console.log('bad conditions');
      throw new errors.Query.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeObject'));
    }
    else {
      console.log('run find');
      return this.where(conditions).all();
    }
  }

  static findOne(conditions) {
    if (!helpers.isPlainObject(conditions)) {
      Promise.reject(new errors.Query.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeObject')));
    }
    else {
      return this.where(conditions).first();
    }
  }

  static query() {
    return new Query(this);
  }

  static query() {
    return new Query(this._model, this);
  }

  _save(query, options) {
    if (query && !(query instanceof Query) && !options) {
      options = query;
      query = null;
    }
    
    if (query && !(query instanceof Query)) {
      Promise.reject(new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeQuery')));
    }
    else if (options && !helpers.isPlainObject(options)) {
      Promise.reject(new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeObject')));
    }
    
    // default options
    options = _.extend({}, options);
    
    // validate
    let validateOptions = _.extend({}, options.validate);
    validateOptions.callbacks = options.callbacks;
    const invalidColumns = this.validate(validateOptions);
    if (invalidColumns) {
      return Promise.reject(new errors.Model.ValidationFailedError(invalidColumns));
    }

    console.log('model _save #1');
  
    // create query
    if (!query) {
      query = new Query(this._model, this);
    }
    
    console.log('model _save #2');
    // don't save if no changes
    if (this._exists && _.size(this._changes) === 0) {
      return Promise.resolve();
    }
    
    console.log('model _save #3');
    // beforeCreate callbacks
    if (!this._exists) {
      if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipBeforeCreate)) {
        this.runCallbacks('beforeCreate');
      }
    }
    
    console.log('model _save #4');
    // beforeSave callbacks
    if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipBeforeSave)) {
      this.runCallbacks('beforeSave');
    }
    
    // wrap callback
    const wrappedCallback = (err, result) => {
      if (err) {
        callback(err);
      }
      else {
        // clear changed fields
        this._prevChanges = this._changes;
        this._changes = {};
        
        // mark as exists
        const created = !this._exists;
        if (!this._exists) {
          this._exists = true;
        }
        
        // afterCreate callbacks
        if (created) {
          if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipAfterCreate)) {
            this.runCallbacks('afterCreate');
          }
        }
        
        // afterSave callbacks
        if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipAfterSave)) {
          this.runCallbacks('afterSave');
        }
        
        callback();
      }
    }
    
    // decide if update or insert
    const update = this._exists || this._model._schema._isCounterColumnFamily;
    
    console.log('model _save #5');
    // only set columns that were changed
    let assignments = {};
    _.each(this._changes, (value, column) => {
      if (!(!this._exists && update && this._model._schema.isKeyColumn(column))) { // don't assign keys in SET clause for UPDATE insertions
        if (!value.op['$set']) {
          const key = _.keys(value.op)[0];
          let assignment = {};
          assignment[key] = lTypes.formatValueType(this._model._orm, this._model._schema.columnType(column), value.op[key]);
          assignments[column] = assignment;
        }
        else {
          assignments[column] = types.formatValueType(this._model._orm, this._model._schema.columnType(column), this._get(column));
        }
      }
    });
    
    console.log('model _save #6');
    // prevent empty SET clause in UPDATE insertions for counter rows by incrementing counters by 0
    if (!this._exists && this._model._schema._isCounterColumnFamily && _.isEmpty(assignments)) {
      _.each(this._model._schema.columns(), (column, index) => {
        if (this._model._schema.columnType(column) === 'counter') {
          assignments[column] = { '$incr': 0 }
        }
      });
    }
    
    console.log('model _save #7');
    // execute query
    if (update) {
      return query.update(assignments).where(primaryKeyConditions.call(this)).execute(wrappedCallback); // todo, fix wrapCallback
    }
    else {
      return query.insert(assignments).execute(wrappedCallback); // todo, fix wrapCallback
    }
  }


  _delete(callback, query, options) {
    if (!_.isFunction(callback)) {
      throw new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeFunction'));
    }
    
    if (query && !(query instanceof Query) && !options) {
      options = query;
      query = null;
    }
    
    if (query && !(query instanceof Query)) {
      throw new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeQuery'));
    }
    else if (options && !lHelpers.isPlainObject(options)) {
      throw new errors.Model.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeObject'));
    }
    
    // default options
    options = _.extend({}, options);
    
    // beforeDelete callbacks
    if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipBeforeDelete)) {
      this.runCallbacks.call(this, 'beforeDelete');
    }
    
    if (!query) {
      query = new Query(this._model, this);
    }

    query.action('delete').where(primaryKeyConditions.call(this)).execute((err, result) => {
      if (err) {
        callback(err);
      }
      else {
        // afterDelete callbacks
        if (!options.callbacks || !(options.callbacks.skipAll || options.callbacks.skipAfterDelete)) {
          this.runCallbacks.call(this, 'afterDelete');
        }
        
        this._exists = false;
        callback();
      }
    });
  }

  static _execute(query, params, options, callback) {
    if (!this._ready) {
      this.addToQueryQueue('execute', arguments);
      return Promise.resolve();
    }
    else {
      return this._orm.execute(query, params, options);
    }
  }

  static _eachRow(query, params, options, modelCallback, completeCallback) {
    if (!this._ready) {
      this.addToQueryQueue('eachRow', arguments);
    }
    else {
      this._orm.eachRow(query, params, options, rowCallback, completeCallback);
    }
  }

  static _stream(query, params, options) {
    if (!this._ready) {
      let stream = new WrappedStream(this);
      this.addToQueryQueue('stream', { stream: stream, args: arguments });
      return stream;
    }
    else {
      return this._orm.stream(this, query, params, options);
    }
  }

  static addToQueryQueue(action, args) {
    if (!this._queryQueue) {
      throw new errors.Model.QueryQueueAlreadyProcessed(i18n.t('errors.orm.general.queryAlreadyProcessed'));
    }
    else if (action !== 'execute' && action !== 'eachRow' && action !== 'stream') {
      throw new errors.Model.InvalidQueryQueueAction(i18n.t('errors.orm.general.invalidAction', {action: action}));
    }
    else {
      this._queryQueue[action].push(args);
    }
  }

  static processQueryQueue() {
    if (!this._queryQueue) {
      throw new errors.Model.QueryQueueAlreadyProcessed(i18n.t('errors.orm.general.queryAlreadyProcessed'));
    }
    else {
      _.each(this._queryQueue, (queue, action) => {
        _.each(queue, (query, index) => {
          if (action === 'execute') {
            this._execute.apply(this, query);
          }
          else if (action === 'eachRow') {
            this._eachRow.apply(this, query);
          }
          else if (action === 'stream') {
            query.stream._setStream(this._stream.apply(this, query.args));
          }
        });
      });

      this._queryQueue = null;
    }
  }

  runCallbacks(key) {
    console.log('AT runCallbacks #1');
    if (!this._model._callbacks[key]) {
      throw new errors.Model.InvalidCallbackKey(i18n.t('errors.orm.general.invalidCallbackKey', {key: key}));
    }
    else {
      console.log('AT runCallbacks #2');
      _.each(this._model._callbacks[key], (callback, index) => {
        console.log('AT runCallbacks #3', callback, index);
        callback.call(this);
        console.log('AT runCallbacks #4', callback, index);
      });
    }
  }

  primaryKeyConditions() {
    let conditions = {};
    
    let partitionKey = this._model._schema.partitionKey();
    if (_.isString(partitionKey)) {
      partitionKey = [partitionKey];
    }

    _.each(partitionKey, (column, index) => {
      const value = this.get(column);
      conditions[column] = _.isUndefined(value) ? null : value;
    });
    
    let clusteringKey = this._model._schema.clusteringKey();
    if (clusteringKey) {
      if (_.isString(clusteringKey)) {
        clusteringKey = [clusteringKey];
      }
      _.each(clusteringKey, (column, index) => {
        const value = this.get(column);
        conditions[column] = _.isUndefined(value) ? null : value;
      });
    }
    
    return conditions;
  }

  static all() {
    let query = new Query(this);
    return query.all.apply(query, arguments);
  }

  static where() {
    let query = new Query(this);
    return query.where.apply(query, arguments);
  }

  static allowFiltering() {
    let query = new Query(this);
    return query.allowFiltering.apply(query, arguments);
  }

  save() {
    console.log('sss save');
    let query = new Query(this._model, this);
    return query.save.apply(query, arguments);
  }
};

// define static query methods
_.each(['select', 'orderBy', 'limit', 'first', 'count', 'eachRow', 'stream', 'truncate', 'deleteAll'], (method, index) => {
  Model[method] = () => {
    console.log('method', method, arguments);
    let query = new Query(this);
    return query[method].apply(query, arguments);
  }
});

 // define instance query methods
_.each(['using', 'ttl', 'timestamp', 'if', 'ifExists', 'ifNotExists', 'delete'], (method, index) => {
  Model.prototype[method] = () => {
    let query = new Query(this._model, this);
    return query[method].apply(query, arguments);
  }
});

export default Model;