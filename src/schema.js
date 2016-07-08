// Libraries
import _ from 'lodash';
import check from 'check-types';
// Modules
import { errors, errorHandler } from './errors';
import * as helpers from './helpers';
import Orm from './index';
import * as types from './types';
import tableWithProperties from './table-with-properties';

const VALID_DEFINITION_FIELDS = [
  'columns',
  'key',
  'with'
];

export default class Schema {
  /**
   * Schema representation for a table in Cassandra.
   * @class
   * @param {Orm} orm The instance of the ORM
   * @param {!Object} definition The schema definition 
   * @param {Object.<string, string>|!Object} definition.columns The set of columns for the schema
   *  which may be a simple column name mapped to its type, or an object containing the type, optional alias, and optional get/set methods
   * @param {Array<string|Array<string>>} definition.key The Table's primary key, composite keys
   *  can be represented by a grouped nested array
   * @param {Object.<string, string>} [definition.with] The WITH condition, i.e. table properties, for use in generating the table
   * @see {@link https://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_table_r.html?scroll=reference_ds_v3f_vfk_xj__setting-a-table-property}
   */
  constructor(orm, definition) {
    /* type-check */
    check.assert.instanceStrict(orm, Orm);
    check.assert.object(definition);
    /* end-type-check */
    /**
     * @type {Object.<string, string>}
     * @name aliases
     * @public
     * @memberOf Schema
     */
    this.aliases = {};
    /**
     * @type {!Object}
     * @name definition
     * @public
     * @memberOf Schema
     */
    this.definition = definition;
    /**
     * @type {Orm}
     * @name orm
     * @private
     * @memberOf Schema
     */
    this.orm = orm;
    /**
     * @type {boolean}
     * @name isCounterColumnFamily
     * @private
     * @memberOf Schema
     */
    this.isCounterColumnFamily = false;
    
    this.validateAndNormalizeDefinition(definition);
  }

  /**
   * Return the list of columns for this schema.
   *
   * @return {Array<string>}
   * @public
   * @function columns
   * @memberOf Schema
   * @instance
   */
  columns() {
    return _.keys(this.definition.columns);
  }

  /**
   * Identifies whether a given column exists in the schema.
   *
   * @param {string} column The name of the column to check for
   * @return {boolean}
   * @public
   * @function isColumn
   * @memberOf Schema
   * @instance
   */
  isColumn(column) {
    /* type-check */
    check.assert.string(column);
    /* end-type-check */
    return !!this.definition.columns[column];
  }

  /**
   * Returns the base type which will strip out any keywords such as 'frozen' 
   * from the type definition.
   *
   * @param {string} column The name of the column
   * @return {string}
   * @public
   * @function baseColumnType
   * @memberOf Schema
   * @instance
   */
  baseColumnType(column) {
    /* type-check */
    check.assert.string(column);
    /* end-type-check */
    return types.baseType(this.orm, this.definition.columns[column].type);
  }

  /**
   * Returns the type in string representation for a given column in the schema.
   *
   * @param {string} column The name of the column
   * @return {?string}
   * @public
   * @function columnType
   * @memberOf Schema
   * @instance
   */
  columnType(column) {
    /* type-check */
    check.assert.string(column);
    /* end-type-check */
    if (this.definition.columns[column]) {
      return this.definition.columns[column].type;
    }

    return null;
  }

  /**
   * Identifies whether a given value is compatible with the column's type
   * in the schema.
   *
   * @param {string} column The name of the column
   * @param {*} value The value to compare against the column's type
   * @return {boolean}
   * @public
   * @function isValidValueTypeForColumn
   * @memberOf Schema
   * @instance
   */
  isValidValueTypeForColumn(column, value) {
    /* type-check */
    check.assert.string(column);
    /* end-type-check */
    return types.isValidValueType(this.orm, this.columnType(column), value);
  }
  
  /**
   * Utility method to get the `get` method on a column's definition.
   *
   * @param {string} column The name of the column
   * @return {?function(*): *}
   * @public
   * @function columnGetter
   * @memberOf Schema
   * @instance
   */
  columnGetter(column) {
    /* type-check */
    check.assert.string(column);
    /* end-type-check */
    if (this.definition.columns[column]) {
      return this.definition.columns[column].get;
    }
    
    return null;
  }
  
  /**
   * Utility method to get the `set` method on a column's definition.
   *
   * @param {string} column The name of the column
   * @return {?function(*): *}
   * @public
   * @function columnSetter
   * @memberOf Schema
   * @instance
   */
  columnSetter(column) {
    /* type-check */
    check.assert.string(column);
    /* end-type-check */
    if (this.definition.columns[column]) {
      return this.definition.columns[column].set;
    }

    return null;
  }
  
  /**
   * Utility method to get the column's alias, if one exists.
   *
   * @param {string} column The name of the column
   * @return {?string}
   * @public
   * @function columnAlias
   * @memberOf Schema
   * @instance
   */
  columnAlias(column) {
    /* type-check */
    check.assert.string(column);
    /* end-type-check */
    if (this.definition.columns[column]) {
      return this.definition.columns[column].alias;
    }

    return null;
  }
  
  /**
   * Identifies whether a given alias matches the alias for a column
   * in the schema.
   *
   * @param {string} alias The alias name
   * @return {boolean}
   * @public
   * @function isAlias
   * @memberOf Schema
   * @instance
   */
  isAlias(alias) {
    return !!this.aliases[alias];
  }
  
  /**
   * Utility method to get the column name matching a given alias, if one exists.
   *
   * @param {string} column The alias name
   * @return {?string}
   * @public
   * @function columnFromAlias
   * @memberOf Schema
   * @instance
   */
  columnFromAlias(alias) {
    return this.aliases[alias];
  }
  
  /**
   * Utility method to get the partition key defined for this schema, i.e. the
   * primary key for the table
   *
   * @return {?string}
   * @public
   * @function partitionKey
   * @memberOf Schema
   * @instance
   */
  partitionKey() {
    return this.definition.key[0];
  }
  
  /**
   * Utility method to get the key used for clustering for this schema.
   *
   * @return {?string}
   * @public
   * @function partitionKey
   * @memberOf Schema
   * @instance
   * @see {@link https://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_table_r.html?scroll=reference_ds_v3f_vfk_xj__using-a-composite-partition-key}
   */
  clusteringKey() {
    const key = _.slice(this.definition.key, 1);
    if (key.length > 1) {
      return key;
    }
    else if (key.length === 1) {
      return key[0];
    }
    else {
      return false;
    }
  }

  /**
   * Utility method to determine if a given column if part of the partition key.
   *
   * @param {string} column The name of the column
   * @return {boolean}
   * @public
   * @function isKeyColumn
   * @memberOf Schema
   * @instance
   */
  isKeyColumn(column) {
    /* type-check */
    check.assert.string(column);
    /* end-type-check */
    return _.flatten(this.definition.key).indexOf(column) > -1;
  }
  
  /**
   * Utility method to get the WITH condition used to generate the table.
   *
   * @param {string} column The name of the column
   * @return {boolean}
   * @public
   * @function isKeyColumn
   * @memberOf Schema
   * @instance
   */
  with() {
    return this.definition.with;
  }

  /**
   * Validates the schema definition object. 
   *
   * @param {!Object} definition
   * @private
   * @function validateAndNormalizeDefinition
   * @memberOf Schema
   * @instance
   * @throws {errors.InvalidSchemaDefinitionKey}
   * @throws {errors.MissingDefinition}
   * @see {@link Schema#constuctor} for more details
   */
  validateAndNormalizeDefinition(definition) {
    /* type-check */
    check.assert.object(definition);
    /* end-type-check */
    _.each(definition, (value, key) => {
      if (VALID_DEFINITION_FIELDS.indexOf(key) === -1) {
        throw new errors.InvalidSchemaDefinitionKey(`Unknown schema definition key: ${key}`);
      }
    });
    
    if (!definition.columns) {
      throw new errors.MissingDefinition('Schema must define columns.');
    }
    else {
      this.validateAndNormalizeColumns(definition.columns);
    }
    
    if (!definition.key) {
      throw new errors.MissingDefinition('Schema must define a key.');
    }
    else {
      this.validateAndNormalizeKey(definition.key);
    }
    
    if (definition.with) {
      this.validateAndNormalizeWith(definition.with);
    }
  }
  
  /**
   * Validates the column definitions. 
   *
   * @param {!Object} columns
   * @private
   * @function validateAndNormalizeColumns
   * @memberOf Schema
   * @instance
   * @throws {errors.InvalidTypeDefinition}
   * @throws {errors.InvalidGetterSetterDefinition}
   * @throws {errors.InvalidAliasDefinition}
   * @throws {errors.InvalidColumnDefinitionKey}
   * @see {@link Schema#constuctor} for more details
   */
  validateAndNormalizeColumns(columns) {
    /* type-check */
    check.assert.object(columns);
    /* end-type-check */
    _.each(columns, (definition, column) => {
      // normalize
      if (_.isString(definition)) {
        definition = { type: definition };
        columns[column] = definition;
      }
      
      /* type-check */
      check.assert.object(definition);
      /* end-type-check */
      
      if (!definition.type) {
        throw new errors.InvalidTypeDefinition(`Type must be defined in column: ${column} schema.`);
      }
      
      _.each(definition, (value, key) => {
        // type
        if (key === 'type') {
          if (!value || !_.isString(value)) {
            throw new errors.InvalidTypeDefinition(`Type: ${value} should be a string in column: ${column} schema.`);
          }
          else {
            definition.type = value = types.sanitize(value);
            if (!types.isValidType(this.orm, value)) {
              throw new errors.InvalidTypeDefinition(`Invalid type: ${value} in column: ${column} schema.`);
            }
            
            // mark counter column family
            if (value === 'counter') {
              this.isCounterColumnFamily = true;
            }
          }
        }
        else if (key === 'set' || key === 'get') {
          if (value && !_.isFunction(value)) {
            throw new errors.InvalidGetterSetterDefinition(`Setter / getters should be functions in column: ${column} schema.`);
          }
        }
        else if (key === 'alias') {
          if (value && !_.isString(value)) {
            throw new errors.InvalidAliasDefinition(`Alias should be a string in column: ${column} schema.`);
          }
          else if (this.aliases[value] || columns[value]) {
            throw new errors.InvalidAliasDefinition(`Alias conflicts with another alias or column name in column: ${column} schema.`);
          }
          else {
            this.aliases[value] = column;
          }
        }
        else {
          throw new errors.InvalidColumnDefinitionKey(`Invalid column definition key: ${key} in column: ${column} schema.`);
        }
      });
    });
  }
  
  /**
   * Validates the partition key definition. 
   *
   * @param {Array<string|Array<string>> key
   * @private
   * @function validateAndNormalizeKey
   * @memberOf Schema
   * @instance
   * @throws {errors.InvalidKeyDefinition}
   * @throws {errors.InvalidType}
   * @see {@link Schema#constuctor} for more details
   */
  validateAndNormalizeKey(key) {
    /* type-check */
    check.assert.array(key);
    /* end-type-check */
    _.each(key, (column, index) => {
      if (_.isArray(column)) {
        if (index != 0) {
          throw new errors.InvalidKeyDefinition('Composite key can only appear at beginning of key definition.');
        }
        else {
          _.each(column, (c, i) => {
            if (!this.isColumn(c)) {
              throw new errors.InvalidKeyDefinition('Key refers to invalid column.');
            }
          });
        }
      }
      else if (!_.isString(column)) {
        throw new errors.InvalidType('Type should be a string.');
      }
      else if (!this.isColumn(column)) {
        throw new errors.InvalidKeyDefinition('Key refers to invalid column.');
      }
    });
  }
 
  /**
   * Validates the WITH, table properties, definition. 
   *
   * @param {Object.<string, !Object> properties
   * @private
   * @function validateAndNormalizeWith
   * @memberOf Schema
   * @instance
   * @throws {errors.InvalidWithDefinition}
   * @see {@link Schema#constuctor} for more details
   */
  validateAndNormalizeWith(properties) {
    /* type-check */
    check.assert.object(properties);
    /* end-type-check */
    _.each(properties, (value, property) => {
      if (!tableWithProperties.PROPERTIES[property]) {
        throw new errors.InvalidWithDefinition(`Invalid with property: ${property}.`);
      }
      else if (property === '$clustering_order_by') {
        const clusteringKey = this.clusteringKey();
        _.each(value, (order, column) => {
          if (!tableWithProperties.CLUSTERING_ORDER[order]) {
            throw new errors.InvalidWithDefinition(`Invalid with clustering order: ${order}.`);
          }
          else {
            if (!clusteringKey || (_.isArray(clusteringKey) && indexOf(clusteringKey, column) === -1) || clusteringKey !== column) {
              throw new errors.InvalidWithDefinition(`Invalid with clustering column: ${column}.`);
            }
          }
        });
      }
    });
  }
  
  /**
   * Utility function to mixin model attributes based on the schema definition.
   *
   * @ignore
   */
  mixin(model) {
    this.mixinGettersAndSetters(model);
    this.mixinTypeSpecificSetters(model);
  }
  
  /**
   * Utility function to add to a model get/set functions based on the columns
   * in the definition.
   *
   * @ignore
   */
  mixinGettersAndSetters(model) {
    _.each(this.columns(), (column, index) => {
      let name = model.options.getterSetterName(column);
      if (!_.isUndefined(model.prototype[name]) && name !== 'name') { // explicitly allow overriding name property
        ErrorHandler.logWarn(`Getter, setter name conflicts with existing property name: ${name} in ${model.name}.`);
        name = 'get_set_' + name;
        ErrorHandler.logWarn(`Defining getter, setter as ${name}.`);
      }

      this.defineGetterSetter(model, name, column);
      
      // alias
      const alias = this.columnAlias(column);
      if (alias) {
        let aliasName = model.options.getterSetterName(alias);
        if (!_.isUndefined(model.prototype[aliasName]) && aliasName !== 'name') { // explicitly allow overriding name property
          ErrorHandler.logWarn(`Alias getter, setter name conflicts with existing property name: ${name} in ${model.name}.`);
          aliasName = 'get_set_' + aliasName;
          ErrorHandler.logWarn(`Defining alias getter, setter as ${aliasName}.`);
        }
        this.defineGetterSetter(model, aliasName, column);
      }
    });
  }
  
  /**
   * Setups an attribute on the model which matches the column name
   * with basic get and set capabilities.
   *
   * @ignore
   */
  defineGetterSetter(model, name, column) {
    Object.defineProperty(model.prototype, name, {
      get: () => {
        return this.get(column);
      },
      set: (value) => {
        this.set(column, value);
      }
    });
  }
  
  /**
   * Provides specific modifiers for attributes added to the model
   * based on the type.
   * @ignore
   */
  mixinTypeSpecificSetters(model) {
    _.each(this.columns(), (column, index) => {
      let operations = [];
      const type = this.baseColumnType(column);
      if (type === 'list') {
        operations = ['append', 'prepend', 'remove', 'inject'];
      }
      else if (type === 'set') {
        operations = ['add', 'remove'];
      }
      else if (type === 'map') {
        operations = ['inject', 'remove'];
      }
      else if (type === 'counter') {
        operations = ['increment', 'decrement'];
      }

      if (operations.length > 0) {
        _.each(operations, (operation, index) => {
          // column
          let name = model.options.typeSpecificSetterName(operation, column);
          if (!_.isUndefined(model.prototype[name])) {
            ErrorHandler.logWarn(`Type specific setter name conflicts with existing property name: ${name} in ${model.name}.`);
            name = 'specific_' + name;
            ErrorHandler.logWarn(`Defining setter as ${name}.`);
          }
          
          // alias
          const alias = this.columnAlias(column);
          let aliasName = false;
          if (alias) {
            aliasName = model.options.typeSpecificSetterName(operation, alias);
            if (!_.isUndefined(model.prototype[aliasName])) {
              ErrorHandler.logWarn(`Type specific alias setter name conflicts with existing property name: ${aliasName} in ${model.name}.`);
              aliasName = 'specific_' + aliasName;
              ErrorHandler.logWarn(`Defining setter as ${name}.`);
            }
          }
          
          if (operation === 'inject') {
            model.prototype[name] = (key, value) => {
              this.inject(column, key, value);
            };
            
            if (alias) {
              model.prototype[aliasName] = (key, value) => {
                this.inject(column, key, value);
              };
            }
          }
          else {
            model.prototype[name] = (value) => {
              this[operation](column, value);
            };
            
            if (alias) {
              model.prototype[aliasName] = (value) => {
                this[operation](column, value);
              };
            }
          }
        });
      }
    });
  }
};
