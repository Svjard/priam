// Libraries
import _ from 'lodash';
import check from 'check-types';
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

    // set the initial set of attributes on the model
    if (attrs) {
      this.set(attrs);
    }

    if (!options || !options.skipAfterNewHook) {
      this.afterNew();
    }
  }

  // Callbacks
  afterNew() {}
  beforeCreate() {}
  afterCreate() {}
  beforeValidate() {}
  afterValidate() {}
  beforeSave() {}
  afterSave() {}
  beforeDelete() {}
  afterDelete() {}

  static schema() {
    return null;
  }

  static validations() {
    return null;
  }

  /**
   * Makes sure the table which maps to the model exists.
   *
   * @return {Promise} Resolves once the table is verified to exist or is created, otherwise
   *  rejects with an error
   * @private
   * @ignore
   * @function ensureTable
   * @memberOf Model
   * @instance
   */
  ensureTable() {
    this.table = new Table(this.orm, this.options.tableName(this.name), this.schemaDef, this.options.table);
    return new Promise((resolve, reject) => {
      this.table.ensureExists()
        .then(() => {
          this.ready = true;
          this.processQueryQueue();
          resolve();
        })
        .catch((err) => {
          reject(new errors.EnsureTableExists(`Error trying to ensure table exists: ${err}.`));
        });
    });
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
      columns = this.schemaDef().columns();
    }
    
    let invalidColumns = null;
    _.each(columns, (column, index) => {
      if (!options || !(options.only || options.except) || (options.only && options.only.indexOf(column) > -1) || (options.except && options.except.indexOf(column) === -1)) {
        const messages = this._validate(column);
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
   * Handler for validating a field in the model via the validations.
   *
   * @param {string} column The name of the field in the model
   * @return {(boolean | null)}
   * @private
   * @ignore
   */
  _validate(column) {
    const val = this.get(column);
    if (this.validations) {
      const recipe = this.validationsDef.recipe(column);
      const displayName = this.displayNameFromRecipe(recipe, column);
      return Validations.validate(recipe, val, displayName, instance);
    }
    else {
      return null;
    }
  }

  validateSanitized(column, value, instance) {
    const recipe = instance._validations.recipe(column);
    const displayName = displayNameFromRecipe(recipe, column);
    return Validations.validateSanitized(recipe, value, displayName, instance);
  }
  
  /**
   * Helper method for returning the display name for a given validator's
   * field.
   *
   * @param {Validator} recipe The validator
   * @param {string} column The column name
   * @return {string}
   * @private
   * @ignore
   */
  displayNameFromRecipe(recipe, column) {
    if (recipe.displayName) {
      return recipe.displayName;
    }
    else {
      return column;
    }
  }

  /**
   * Santizes the value based on the sanitizer setup in the validations for the column if specified.
   *
   * @param {string} column The name of the column being sanitized
   * @param {*} value The value of the column
   * @return {*} The sanitized value
   * @public
   * @function sanitize
   * @memberOf Model
   * @instance
   */
  sanitize(column, value) {
    if (this.validations()) {
      const recipe = this.validations().recipe(column);
      return Validations.sanitize(recipe, value, this);
    } else {
      return value;
    }
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
    if (!this.upsert && this.exists && this.schema.isKeyColumn()) {
      throw new errors.CannotSetKeyColumns(`Columns in primary key cannot be modified once set: ${column}.`);
    }

    if (helpers.isPlainObject(column)) {
      _.each(column, (v, c) => {
        this._set(c, v);
      });
    }
    else if (_.isString(column)) {
      this._set(column, value);
    }
  }

  /**
   * Sets the value of a column. This serves as a private helper function to process the `set`
   * and is internal to the Model.
   *
   * @param {string|Object<String, *>} column The name of the column or map of columns
   *  and values to be set
   * @param {*} [value] The value to set the value to
   * @public
   * @function _set
   * @memberOf Model
   * @instance
   */
  _set(column, value) {
    if (this.schemaDef.isAlias(column)) {
      column = this.schemaDef.columnFromAlias(column);
    }
    
    if (!this.schemaDef.isColumn(column)) {
      throw new errors.InvalidColumnError(`Invalid column: ${column}.`);
    }
    else if (this.schemaDef.baseColumnType(column) === 'counter') {
      throw new errors.CannotSetCounterColumnsError(`Counter column: ${column} cannot be set directly. Increment or decrement instead.`);
    }
    
    const prevValue = this.changes[column] ? this.changes[column].prev : this.get(column);
    
    // sanitize
    value = this.sanitize(column, value, this);
    
    // schema setter
    const setter = this.model.schema.columnSetter(column);
    if (setter) {
      value = setter.call(this, value);
    }
    
    // cassandra treats empty sets and lists as null values
    if (_.isArray(value) && value.length === 0) {
      value = null;
    }
    
    if (!_.isNull(value)) { // allow null values
      // cast string type to javascript types
      if (_.isString(value)) {
        const type = this.model.schema.baseColumnType(column);
        if (types.isNumberType(this.model.orm, type)) {
          value = parseFloat(value.replace(/[^\d\.\-]/g, ''));
          if (_.isNaN(value)) {
            value = null;
          }
        }
        else if (types.isBooleanType(this.model.orm, type)) {
          value = value !== '0' && value !== 'false' && value;
        }
      }
      // cast cassandra types to javascript types
      else {
        value = types.castValue(this.model.orm, value);
      }

      // validate type
      // recheck null, since casting can cast to null
      if (!_.isNull(value) && !this.model.schema.isValidValueTypeForColumn(column, value)) {
        throw new errors.TypeMismatch(`Value for ${column} should be of type: ${this.model.schema.columnType(column)}.`);
      }

      // make set array uniq
      if (this.model.schema.baseColumnType(column) === 'set') {
        value = helpers.uniq(value);
      }
    }
    
    this.columns[column] = value;
    
    if (this.upsert && this.model.schema.isKeyColumn(column)) {
      return;
    } else {
      value = this.get(column);
      if (!helpers.isEqual(value, prevValue)) {
        this.changes[column] = { prev: prevValue, op: { '$set': true } };
      }
      else {
        delete this.changes[column];
      }
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
        columns[c] = this._get(c);
      });
      return columns;
    }
    else if (_.isString(column)) {
      return this._get(column);
    }
    else {
      throw new errors.InvalidArgument('Column name should be a string');
    }
  }
  
  /**
   * Gets the value of a column. This serves as a private helper function to process the `get`
   * and is internal to the Model.
   *
   * @param {string} column The name of the column
   * @private
   * @function _get
   * @memberOf Model
   * @instance
   */
  _get(column) {
    /* type-check */
    check.assert.nonEmptyString(colum);
    /* end-type-check */
    if (!this.model.schema.isColumn(column)) {
      throw new errors.InvalidColumn(`Invalid column: ${column}.`);
    }

    if (this.upsert && !this.model.schema.isKeyColumn(column)) {
      if (!this.changes[column]) {
        throw new errors.IndeterminateValue(`Reading value not previously set for column: ${column}.`);
      }
      else if (!this.changes[column].op['$set']) {
        throw new errors.IndeterminateValue(`Reading value modified by idempotent operations for column: ${column}.`);
      }
    }

    let value = this.columns[column];

    // schema getter
    const getter = this.model.schema.columnGetter(column);
    if (getter) {
      value = getter.call(this, value);
    }
    
    // cast undefined
    if (_.isUndefined(value)) {
      // cast counters to 0
      if (this.model.schema.columnType(column) === 'counter') {
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
    
    this.append('$add', column, value); // TODO
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

  /**
   * Prepends a value to a list for a given column.
   *
   * @param {string} column The name of the column
   * @param {*} value The value to prepend to the list collection
   * @public
   * @function prepend
   * @memberOf Model
   * @instance
   */
  prepend(column, value) {
    /* type-check */
    check.assert.nonEmtpyString(column);
    /* end-type-check */
    if (!this.model.schema.isColumn(column)) {
      throw new errors.InvalidColumn(`Invalid column: ${column}.`);
    }
    else if (this.model.schema.baseColumnType(column) !== 'list') {
      throw new errors.InvalidColumnType('Prepend can only be performed on columns of type list.');
    }
    
    // if update, only record idempotent operations
    if (this.upsert) {
      if (this.changes[column]) {
        if (this.changes[column].op['$prepend']) {
          this.changes[column].op['$prepend'].push(value);
        }
        else {
          throw new errors.OperationConflict(`Multiple conflicting operations on column: ${column}.`);
        }
      }
      else {
        this.changes[column] = { op: { '$prepend' : [value] } };
      }
      return;
    }
    
    // full set and change tracking
    const prevValue = this.changes[column] ? this.changes[column].prev : this.get(column);
    
    // prepend
    let newValue = this.get(column);
    if (newValue) {
      newValue = [value].concat(newValue); // use concat to return a copy
    }
    else {
      newValue = [value];
    }
    
    this.typeSpecificSet(column, newValue);
    
    // mark column changed
    newValue = this.get(column);
    if (!helpers.isEqual(newValue, prevValue)) {
      if (this.changes[column]) {
        if (this.changes[column].op['$prepend']) {
          this.changes[column].op['$prepend'].push(value);
        }
        else {
          this.changes[column].op = { '$set': true };
        }
      }
      else {
        this.changes[column] = { prev: prevValue, op: { '$prepend' : [value] } };
      }
    }
    else {
      delete this.changes[column];
    }
  }

  /**
   * Appends a value to a list for a given column.
   *
   * @param {string} column The name of the column
   * @param {*} value The value to append to the list collection
   * @public
   * @function append
   * @memberOf Model
   * @instance
   */
  append(column, value) {
    /* type-check */
    check.assert.nonEmtpyString(column);
    /* end-type-check */
    if (!this.model.schema.isColumn(column)) {
      throw new errors.InvalidColumn(`Invalid column: ${column}.`);
    }
    else if (this.model.schema.baseColumnType(column) !== 'list') {
      throw new errors.Model.InvalidColumnType('Append can only be performed on columns of type list.');
    }
    
    this.append('$append', column, value); // TODO
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