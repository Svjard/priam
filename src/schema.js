// Libraries
import _ from 'lodash';
// @TODO replace
import nm_s from 'underscore.string';
// Modules
import { errors, errorHandler } from './errors';
import Orm from './index';
import * as helpers from './helpers';
import * as types from './types';
import tableWithProperties from './table-with-properties';
import i18n from '../i18n';

class Schema {
  constructor(orm, definition) {
    this.orm = orm;
    this.aliases = {};
    this.definition = definition;
    this.isCounterColumnFamily = false;
    
    this.validateAndNormalizeDefinition(definition); // must be called after setting this._definition
  }

  columns() {
    return _.keys(this.definition.columns);
  }

  isColumn(column) {
    return !!this.definition.columns[column];
  }

  baseColumnType(column) {
    return types.baseType(this.orm, this.definition.columns[column].type);
  }

  columnType(column) {
    return this.definition.columns[column].type;
  }

  isValidValueTypeForColumn(column, value) {
    return types.isValidValueType(this.orm, this.columnType(column), value);
  }

  columnGetter(column) {
    return this.definition.columns[column].get;
  }

  columnSetter(column) {
    return this.definition.columns[column].set;
  }

  columnAlias(column) {
    return this.definition.columns[column].alias;
  }

  isAlias(alias) {
    return !!this.aliases[alias];
  }

  columnFromAlias(alias) {
    return this.aliases[alias];
  }

  partitionKey() {
    return this.definition.key[0];
  }

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

  isKeyColumn(column) {
    return _.flatten(this.definition.key).indexOf(column) > -1;
  }
  
  with() {
    return this.definition.with;
  }

  addCallback(key, callback) {
    if (!this.definition.callbacks) {
      this.definition.callbacks = {};
    }

    if (!this.definition.callbacks[key]) {
      this.definition.callbacks[key] = [];
    }

    this.definition.callbacks[key].push(callback);
  }

  validateAndNormalizeDefinition(definition) {
    _.each(definition, (value, key) => {
      if (key !== 'columns' && key !== 'key' && key !== 'with' && key !== 'callbacks' && key !== 'methods' && key !== 'staticMethods') {
        throw new errors.Schema.InvalidSchemaDefinitionKey(i18n.t('errors.orm.general.unknownSchemaKey', {key: key}));
      }
    });
    
    if (!definition.columns) {
      throw new errors.Schema.MissingDefinition(i18n.t('errors.orm.general.schemaMissingColumns'));
    }
    else {
      this.validateAndNormalizeColumns.call(this, definition.columns);
    }
    
    if (!definition.key) {
      throw new errors.Schema.MissingDefinition(i18n.t('errors.orm.general.schemaMissingKey'));
    }
    else {
      this.validateAndNormalizeKey.call(this, definition.key);
    }
    
    if (definition.with) {
      this.validateAndNormalizeWith.call(this, definition.with);
    }
    
    if (definition.callbacks) {
      this.validateAndNormalizeCallbacks.call(this, definition.callbacks);
    }
    
    if (definition.methods) {
      this.validateAndNormalizeMethods.call(this, definition.methods);
    }
    
    if (definition.staticMethods) {
      this.validateAndNormalizeStaticMethods.call(this, definition.staticMethods);
    }
  }

  validateAndNormalizeColumns(columns) {
    if (!helpers.isPlainObject(columns)) {
      throw new errors.Schema.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeObject'));
    }
    else {
      _.each(columns, (definition, column) => {
        // normalize
        if (_.isString(definition)) {
          definition = { type: definition };
          columns[column] = definition;
        }
        
        // validate definition
        if (!helpers.isPlainObject(definition)) {
          throw new errors.Schema.InvalidType(i18n.t('errors.orm.types.shouldBeObject'));
        }
        else if (!definition.type) {
          throw new errors.Schema.InvalidTypeDefinition(i18n.t('errors.orm.types.columnRequiresType', {column: column}));
        }
        
        _.each(definition, (value, key) => {
          // type
          if (key === 'type') {
            if (!value || !_.isString(value)) {
              throw new errors.Schema.InvalidTypeDefinition(i18n.t('errors.orm.types.columnMustBeString', {value: value, column: column}));
            }
            else {
              definition.type = value = types.sanitize(this._orm, value);
              if (!types.isValidType(this._orm, value)) {
                throw new errors.Schema.InvalidTypeDefinition(i18n.t('errors.orm.types.invalidType', {value: value, column: column}));
              }
              
              // mark counter column family
              if (value === 'counter') {
                this._isCounterColumnFamily = true;
              }
            }
          }
          else if (key === 'set' || key === 'get') {
            if (value && !_.isFunction(value)) {
              throw new errors.Schema.InvalidGetterSetterDefinition(i18n.t('errors.orm.general.invalidGetSet', {column: column}));
            }
          }
          else if (key === 'alias') {
            if (value && !_.isString(value)) {
              throw new errors.Schema.InvalidAliasDefinition(i18n.t('errors.orm.general.aliasMustBeString', {column: column}));
            }
            else if (this._aliases[value] || columns[value]) {
              throw new errors.Schema.InvalidAliasDefinition(i18n.t('errors.orm.general.duplicateAlias', {column: column}));
            }
            else {
              this._aliases[value] = column;
            }
          }
          else {
            throw new errors.Schema.InvalidColumnDefinitionKey(i18n.t('errors.orm.general.invalidColumnDefinition', {key: key, column: column}));
          }
        });
      });
    }
  }

  validateAndNormalizeKey(key) {
    if (!_.isArray(key)) {
      throw new errors.Schema.InvalidArgument(i18n.t('errors.orm.arguments.shouldBeArray'));
    }
    else {
      _.each(key, (column, index) => {
        if (_.isArray(column)) {
          if (index != 0) {
            throw new errors.Schema.InvalidKeyDefinition(i18n.t('errors.orm.general.invalidCompositeKey'));
          }
          else {
            _.each(column, (c, i) => {
              if (!this.isColumn(c)) {
                throw new errors.Schema.InvalidKeyDefinition(i18n.t('errors.orm.general.invalidKeyColumn'));
              }
            });
          }
        }
        else if (!_.isString(column)) {
          throw new errors.Schema.InvalidType(i18n.t('errors.orm.types.shouldBeString'));
        }
        else if (!this.isColumn(column)) {
          throw new errors.Schema.InvalidKeyDefinition(i18n.t('errors.orm.general.invalidKeyColumn'));
        }
      });
    }
  }

  validateAndNormalizeWith(properties) {
    _.each(properties, (value, property) => {
      if (!tableWithProperties.PROPERTIES[property]) {
        throw new errors.Schema.InvalidWithDefinition(i18n.t('errors.orm.general.invalidProperty', {property: property}));
      }
      else if (property === '$clustering_order_by') {
        const clusteringKey = this.clusteringKey();
        _.each(value, (order, column) => {
          if (!tableWithProperties.CLUSTERING_ORDER[order]) {
            throw new errors.Schema.InvalidWithDefinition(i18n.t('errors.orm.general.invalidClustingOrder', {order: order}));
          }
          else {
            if (!clusteringKey || (_.isArray(clusteringKey) && indexOf(clusteringKey, column) === -1) || clusteringKey !== column) {
              throw new errors.Schema.InvalidWithDefinition(i18n.t('errors.orm.general.invalidClustingColumn', {column: column}));
            }
          }
        });
      }
      i++;
    });
  }

  validateAndNormalizeCallbacks(callbacks) {
    _.each(callbacks, (c, key) => {
      if (Schema._CALLBACK_KEYS.indexOf(key) < 0) {
        throw new errors.Schema.InvalidCallbackKey(i18n.t('errors.orm.general.invalidCallbackKey'));
      }
      else {
        // normalize
        if (_.isFunction(c)) {
          c = [c];
          callbacks[key] = c;
        }
        
        if (!_.isArray(c)) {
          throw new lErrors.Schema.InvalidType(i18n.t('errors.orm.types.shouldBeArray'));
        }
        else {
          _.each(c, (func, index) => {
            if (!_.isFunction(func)) {
              throw new errors.Schema.InvalidType(i18n.t('errors.orm.types.shouldBeFunction'));
            }
          });
        }
      }
    });
  }

  validateAndNormalizeMethods(methods) {
    _.each(methods, (method, key) => {
      if (!_.isFunction(method)) {
        throw new errors.Schema.InvalidType(i18n.t('errors.orm.types.shouldBeFunction'));
      }
    });
  }

  validateAndNormalizeStaticMethods(staticMethods) {
    _.each(staticMethods, (static_method, key) => {
      if (!_.isFunction(static_method)) {
        throw new errors.Schema.InvalidType(i18n.t('errors.orm.types.shouldBeFunction'));
      }
    });
  }

  mixin(model) {
    console.log('mixin', model);
    this.mixinGettersAndSetters(model);
    this.mixinTypeSpecificSetters(model);
    this.mixinCallbacks(model);
    this.mixinMethods(model);
    this.mixinStaticMethods(model);
  }

  mixinGettersAndSetters(model) {
    _.each(this.columns(), (column, index) => {
      // column
      let name = model._options.getterSetterName(column);
      if (!_.isUndefined(model.prototype[name]) && name !== 'name') { // explicitly allow overriding name property
        errorHandler.logWarn(i18n.t('warnings.orm.conflictingPropertyName', {name: name, model: model._name}));
        name = 'get_set_' + name;
        errorHandler.logWarn(i18n.t('warnings.orm.redefineProperty', {name: name}));
      }
      this.defineGetterSetter(model, name, column);
      
      // alias
      const alias = this.columnAlias(column);
      if (alias) {
        let aliasName = model._options.getterSetterName(alias);
        if (!_.isUndefined(model.prototype[aliasName]) && aliasName !== 'name') { // explicitly allow overriding name property
          errorHandler.logWarn(i18n.t('warnings.orm.conflictingAliasName', {name: aliasName, model: model._name}));
          aliasName = 'get_set_' + aliasName;
          errorHandler.logWarn(i18n.t('warnings.orm.redefineAlias', {name: aliasName}));
        }
        this.defineGetterSetter(model, aliasName, column);
      }
    });
  }

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
          let name = model._options.typeSpecificSetterName(operation, column);
          if (!_.isUndefined(model.prototype[name])) {
            errorHandler.logWarn(i18n.t('warnings.orm.conflictingSetter', {name: name, model: model._name}));
            name = 'specific_' + name;
            errorHandler.logWarn(i18n.t('warnings.orm.definedSetter', {name: name}));
          }
          
          // alias
          const alias = this.columnAlias(column);
          let aliasName = false;
          if (alias) {
            aliasName = model._options.typeSpecificSetterName(operation, alias);
            if (!_.isUndefined(model.prototype[aliasName])) {
              errorHandler.logWarn(i18n.t('warnings.orm.conflictingAliasSetter', {name: aliasName, model: model._name}));
              aliasName = 'specific_' + aliasName;
              errorHandler.logWarn(i18n.t('warnings.orm.definedSetter', {name: name}));
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

  mixinCallbacks(model) {
    if (this._definition.callbacks) {
      _.each(this._definition.callbacks, (callbacks, key) => {
        model._callbacks[key].push.apply(model._callbacks[key], callbacks);
      });
    }
  }

  mixinMethods(model) {
    console.log('methods', this._definition);
    if (this._definition.methods) {
      _.each(this._definition.methods, (method, key) => {
        if (!_.isUndefined(model.prototype[key]) && (key !== 'name' && !this.isColumn(key))) { // explicitly allow overriding name property
          errorHandler.logWarn(i18n.t('warnings.orm.conflictingMethodName', {key: key, model: model._name}));
          key = 'method_' + key;
          errorHandler.logWarn(i18n.t('warnings.orm.definedMethod', {key: key}));
        }
        model.prototype[key] = method;
      });
    }
  }

  mixinStaticMethods(model) {
    if (this._definition.staticMethods) {
      _.each(this._definition.staticMethods, (static_method, key) => {
        if (!_.isUndefined(model[key])  && key !== 'name') { // explicitly allow overriding name property
          errorHandler.logWarn(i18n.t('warnings.orm.conflictingStaticMethodName', {key: key, model: model._name}));
          key = 'method_' + key;
          errorHandler.logWarn(i18n.t('warnings.orm.definedMethod', {key: key}));
        }
        model[key] = static_method;
      });
    }
  }
};

Schema._CALLBACK_KEYS = ['afterNew', 'beforeCreate', 'afterCreate', 'beforeValidate', 'afterValidate', 'beforeSave', 'afterSave', 'beforeDelete', 'afterDelete'];
_.each(Schema._CALLBACK_KEYS, (key, index) => {
  let methodName = 'add' + nm_s.capitalize(key) + 'Callback';
  Schema.prototype[methodName] = (callback) => {
    addCallback.call(this, key, callback);
  };
});

export default Schema;