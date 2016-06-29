// Libraries
import _ from 'lodash';
import Promise from 'bluebird';
// Modules
import { errors, ErrorHandler } from './errors';
import * as helpers from './helpers';
import types from './types';
import Orm from './index';

function hexEncode(str) {
  let hex;
  let result = '';
  for (let i = 0; i < str.length; i++) {
    hex = str.charCodeAt(i).toString(16);
    result += hex;
  }
  return result
}

/**
 * Defines a Cassandra user-defined type
 * @class
 */
export default class UserDefinedType {
  /**
   * @constructor
   * @param {Orm} orm The instance of the ORM
   * @param {string} name The name of the user-defined type
   * @param {Object} definition The type definition as an associative array of field name to field types
   * @param {Object} options The options set from the Cassandra config, i.e. config.js
   */
  constructor(orm, name, definition, options) {
    this.orm = orm;
    this.name = name;
    this.definition = definition;
    this.options = options;
    
    this.validateAndNormalizeDefinition(definition);
  }

  /**
   * Fetches all the fields in the user-defined type.
   * @return {Array<string>}
   */
  fields() {
    return _.keys(this.definition);
  }

  /**
   * Determines if a given field exists in the user-defined type.
   * @return {boolean}
   */
  isField(field) {
    return !!this.definition[field];
  }

  /**
   * Gets the type for a given field.
   *
   * @param {string} field The name of the field 
   * @return {string}
   */
  fieldType(field) {
    return this.definition[field];
  }

  /**
   * Determines if a value is a valid match for a specific field's type.
   *
   * @param {string} field The name of the field 
   * @param {*} value The value to check against the field's type 
   * @return {boolean}
   */
  isValidValueTypeForField(field, value) {
    return types.isValidValueType(this.orm, this.fieldType(field), value);
  }

  /**
   * Determines if a value is a valid match against the user-defined type.
   *
   * @param {*} value The value to check against
   * @return {boolean}
   */
  isValidValueTypeForSelf(value) {
    // Need to iterate over the fields in the type and check
    // field by field
    for (let field in value) {
      const type = this.fieldType(field);
      if (!type || !types.isValidValueType(this.orm, type, value[field])) {
        return false;
      }
    }

    return true;
  }

  /**
   * Determines if a value is a valid match against the user-defined type.
   *
   * @param {*} value The value to check against
   * @return {boolean}
   */
  formatValueTypeForSelf(value) {
    _.each(value, (v, field) => {
      let type = this.fieldType(field);
      value[field] = types.formatValueType(this._orm, type, v);
    });
    return value;
  }

  /**
   * Defines the user-defined type as native Cassandra Java type.
   *
   * @return {string}
   */
  dbValidator() {
    let validator = this.orm.keyspace;
    validator += ',' + hexEncode(this.name);
    _.each(this.definition, (type, field) => {
      validator += ',' + hexEncode(field) + ':' + types.dbValidator(this.orm, type, true);
    });
    validator = 'org.apache.cassandra.db.marshal.UserType(' + validator + ')';
    return validator;
  }
  
  /**
   * Validates the definition for invalid syntax and field types.
   *
   * @param {Object} definition The type definition as an associative array of field name to field types
   * @throws {InvalidFieldDefinition}
   */
  validateAndNormalizeDefinition(definition) {
    _.each(definition, (type, field) => {
      if (!_.isString(type)) {
        throw new errors.InvalidFieldDefinitionError(i18n.t('errors.orm.general.invalidTypeForField', { type: type, keyspace: this.orm.keyspace, field: field }));
      }

      if (type.indexOf('<') > -1 && type.indexOf('frozen') !== 0) {
        throw new errors.InvalidFieldDefinitionError(i18n.t('errors.orm.general.collectionsMustBeFrozen', { type: type, keyspace: this.orm.keyspace, field: field }));
      }
      else {
        type = types.sanitize(this.orm, type);
        definition[field] = type;
        if (!types.isValidType(this.orm, type)) {
          throw new errors.InvalidFieldDefinitionError(i18n.t('errors.orm.general.invalidTypeForField', { type: type, keyspace: this.orm.keyspace, field: field }));
        }
      }
    });
  }

  /**
   * Using the database configuration settings, will enforce the user-defined type exists
   * in the database.
   *
   * @param {Object} definition The type definition as an associative array of field name to field types
   * @return {Promise}
   * @throws {InvalidFieldDefinition}
   */
  ensureExists(options) {
    return new Promise((resolve, reject) => {
      options = _.extend({ recreate: false, changeType: false, addMissing: false }, this.options.ensureExists, options);
      
      if (!_.isUndefined(options.run) && !options.run) {
        ErrorHandler.logWarn(i18n.t('warnings.orm.udtSkipped', { keyspace: this.orm.keyspace, name: this.name }));
        resolve();
      }
      
      this.selectSchema().then(result => {
        if (!result || !result.rows) {
          reject(new errors.SelectSchemaError(i18n.t('errors.orm.general.schemaNoRows')));
        }

        // create type
        if (result.rows.length === 0) {
          ErrorHandler.logInfo(i18n.t('warnings.orm.creatingType', { keyspace: this.orm.keyspace, name: this.name }));
          this.create({ ifNotExists: true }).then(result => {
            resolve();
          }).catch(err => {
            reject(new errors.CreateError(i18n.t('errors.orm.general.failedToCreateType', { err: err })));
          });
        }
        // compare schema to existing type
        else {
          // create hash for diff
          const fields = {};
          _.each(this.definition, (type, field) => {
            fields[field] = true;
          });
          
          // diff
          const mismatched = [];
          const extra = [];
          result = result.rows[0];
          _.each(result.field_names, (fieldName, index) => {
            if (fields[fieldName]) {
              const type = this.fieldType(fieldName);
              const dbValidator = types.dbValidator(this.orm, type);
              if (dbValidator !== result.field_types[index]) {
                mismatched.push({
                  field: fieldName,
                  expected: dbValidator,
                  actual: result.field_types[index]
                });
              }

              delete fields[fieldName];
            }
            else {
              extra.push(fieldName);
            }
          });

          const missing = _.keys(fields);
          
          // Log the diffs to the console for auditing purposes
          if (mismatched.length > 0) {
            ErrorHandler.logWarn(
              i18n.t('warnings.orm.mismatchedFieldType', { amount: mismatched.length, keyspace: this.orm.keyspace, name: this.name }),
              mismatched
            );
          }

          if (extra.length > 0) {
            ErrorHandler.logWarn(
              i18n.t('warnings.orm.extraFields', { amount: extra.length, keyspace: this.orm.keyspace, name: this.name }),
              extra
            );
          }

          if (missing.length > 0) {
            ErrorHandler.logWarn(
              i18n.t('warnings.orm.missingFields', { amount: missing.length, keyspace: this.orm.keyspace, name: this.name }),
              missing
            );
          }
          
          // Proceed to fix any differences in the type
          if ((mismatched.length > 0 || extra.length > 0 || missing.length > 0) && options.recreate) {
            this.recreate().then(() => resolve());
          }
          else {
            let promises = [];
            if (mismatched.length > 0 && options.changeType) {
              promises = promises.concat(this.fixMismatched(_.map(mismatched, (mismatch, index) => { return mismatch.field })));
            }
            
            if (missing.length > 0 && options.addMissing) {
              promises = promises.concat(this.fixMissing(missing));
            }
            
            Promise.all(promises).then(descriptions => {
              resolve();
            }).catch(err => {
              reject(new errors.FixError(i18n.t('errors.orm.general.failedToFixTable')));
            });
          }
        }
      }).catch(err => {
        reject(new errors.SelectSchemaError(i18n.t('errors.orm.general.errSelectSchema', { err: err })));
      });
    });
  }

  recreate() {
    return new Promise((resolve, reject) => {
      this.drop({ ifExists: true }).then(dropResult => {
        this.create({ ifNotExists: true }).then(createResult => {
          resolve();
        }).catch(err => {
          reject(new errors.FixError(i18n.t('errors.orm.general.failedCreateType', { err: err })));
        });
      }).catch(err => {
        reject(new errors.FixError(i18n.t('errors.orm.general.failedDropType', { err: err })));
      });
    });
  }

  fixMismatched(mismatched) {
    ErrorHandler.logWarn(i18n('warnings.orm.changingFieldTypes'));
    
    const promises = [];
    _.each(mismatched, (field, index) => {
      promises.push(
        new Promise((resolve, reject) => {
          this.alterType(field, this.fieldType(field))
            .then(result => resolve())
            .catch(err => reject(err));
          })
        );
    });
    
    return promises;
  }

  fixMissing(missing) {
    errorHandler.logWarn(i18n('warnings.orm.addMissingFields'));
  
    const promises = [];
    _.each(missing, (field, index) => {
      promises.push(
        new Promise((resolve, reject) => {
          this.addField(field, this.fieldType(field))
            .then(result => resolve())
            .catch(err => reject(err));
        })
      );
    });

    return promises;
  }

  selectSchema() {
    const query = {
      query: 'SELECT * FROM system_schema.types WHERE type_name = ? AND keyspace_name = ? ALLOW FILTERING',
      params: [this.name, this.orm.keyspace],
      prepare: true
    };
    
    return this.orm._system_execute(query.query, query.params, { prepare: query.prepare });
  }

  create(options) {
    options = _.extend({ ifNotExists: false }, options);
    
    const query = {
      query: 'CREATE TYPE',
      params: [],
      prepare: true
    };
    
    if (options.ifNotExists) {
      query.query += ' IF NOT EXISTS';
    }
    
    this.concatBuilders([this.buildTypeName, this.buildFields], query);
    
    return this.orm._system_execute(query.query, query.params, { prepare: query.prepare });
  }

  drop(options) {
    options = _.extend({ ifExists: false }, options);
    
    const query = {
      query: 'DROP TYPE',
      params: [],
      prepare: true
    };
    
    if (options.ifExists) {
      query.query += ' IF EXISTS';
    }
    
    this.concatBuilders([this.buildTypeName], query);
    
    return this.orm._system_execute(query.query, query.params, { prepare: query.prepare });
  }

  // TYPE QUERY OPERATORS

  addField(field, type) {
    const query = {
      query: 'ALTER TYPE',
      params: [],
      prepare: true
    };
    
    this.concatBuilders([this.buildTypeName], query);
    
    query.query += ' ADD "' + field + '" ' + type;
    
    return this.orm._system_execute(query.query, query.params, { prepare: query.prepare });
  }

  renameField(field, newName) {
    // normalize
    let rename = {};
    rename[field] = newName;
    field = rename;
    newName = null;
    
    const query = {
      query: 'ALTER TYPE',
      params: [],
      prepare: true
    };
    
    this.concatBuilders([this.buildTypeName], query);
    
    query.query += ' RENAME';
    let i = 0;
    _.each(field, (newName, currentName) => {
      if (i > 0) {
        query.query += ' AND';
      }
      query.query += ' "' + currentName + '" TO "' + newName + '"';
      i++;
    });
    
    return this.orm._system_execute(query.query, query.params, { prepare: query.prepare });
  }

  alterType(field, type) {    
    const query = {
      query: 'ALTER TYPE',
      params: [],
      prepare: true
    };
    
    this.concatBuilders([this.buildTypeName], query);
    
    query.query += ' ALTER "' + field + '" TYPE ' + type;
    
    return this.orm._system_execute(query.query, query.params, { prepare: query.prepare });
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

  /**
   * Helper method to build out the name of the user-defined type.
   *
   * @return {Object}
   * @private
   */
  buildTypeName() {
    const clause = this.orm.keyspace + '.' + this.name;
    let params = [];
    return { clause: clause, params: params };
  }

  buildFields() {
    let clause = '(';
    let params = [];
    let i = 0;
    _.each(this.definition, (type, field) => {
      if (i > 0) {
        clause += ', ';
      }
      clause += '"' + field + '" ' + type;
      i++;
    });
    clause += ')';
    return { clause: clause, params: params };
  }
}
