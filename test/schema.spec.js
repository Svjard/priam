/*globals describe, beforeEach, afterEach, it*/
import sinon from 'sinon';
import should from 'should';
import _ from 'lodash';
import chalk from 'chalk';
import Orm from '../src';
import Schema from '../src/schema';
import { ErrorHandler, errors } from '../src/errors';

const currentEnv = process.env.NODE_ENV;

const namespace = {
  Schema: Schema
};

const BASE_SCHEMA = {
  columns: {
    ctime: 'timestamp',
    utime: 'timestamp',
    id: 'uuid',
    name: 'text',
    email: {
      alias: 'emailAddress',
      type: 'text',
      set: function(value) { return value.toLowerCase(); },
      get: function(value) { return value.toUpperCase(); }
    },
    ip: 'inet',
    age: 'int',
    friends: 'set<uuid>',
    tags: 'list<text>',
    browsers: 'map<text,inet>',
    craziness: 'list<frozen<tuple<text,int,text>>>'
  },
  key: ['id']
};

describe('ORM :: Schema', () => {
  let sandbox;

  beforeEach(() => {
    sandbox = sinon.sandbox.create();
    ErrorHandler.logWarn = sandbox.spy(ErrorHandler, 'logWarn');
    process.env.NODE_ENV = 'development';
  });

  afterEach(() => {
    sandbox.restore();
    process.env.NODE_ENV = currentEnv;  
  });

  it('should correctly generate a new schema', (done) => {
    let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), BASE_SCHEMA);
    should(schema.definition).deepEqual(BASE_SCHEMA);
    should(Object.keys(schema.aliases).length).equal(1);
    should(schema.aliases.emailAddress).equal('email');
    should(schema.isAlias('emailAddress')).equal(true);
    should(schema.isAlias('invalid')).equal(false);    
    should(schema.columnFromAlias('emailAddress')).equal('email');
    should(schema.columnFromAlias('invalid')).equal(null);
    should(schema.columnAlias('email')).equal('emailAddress');
    should(schema.columnAlias('invalid')).equal(null);
    should(schema.columns()).deepEqual(Object.keys(BASE_SCHEMA.columns));
    Object.keys(BASE_SCHEMA.columns).forEach(key => {
      should(schema.isColumn(key)).equal(true);
      if (key === 'craziness' || key === 'tags') {
        should(schema.baseColumnType(key)).equal('list');
      }
      else if (key === 'friends') {
        should(schema.baseColumnType(key)).equal('set');
      }
      else if (key === 'browsers') {
        should(schema.baseColumnType(key)).equal('map');
      }
      else {
        should(schema.baseColumnType(key)).equal(BASE_SCHEMA.columns[key].type);
      }
      should(schema.columnType(key)).equal(BASE_SCHEMA.columns[key].type);
    });
    should(schema.isColumn('invalid')).equal(false);
    should(schema.columnType('invalid')).equal(null);
    should(schema.columnGetter('email')).not.equal(null);
    should(schema.columnGetter('invalid')).equal(null);
    should(schema.columnSetter('email')).not.equal(null);
    should(schema.columnSetter('invalid')).equal(null); 
    should(schema.columnSetter('invalid')).equal(null);
    should(schema.partitionKey()).equal('id');
    should(schema.with()).equal(null);
    done();
  });

  it('should correctly check types for columns', (done) => {
    let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), BASE_SCHEMA);
    should(schema.isValidValueTypeForColumn('email', 4)).equal(false);
    should(schema.isValidValueTypeForColumn('email', 'abc')).equal(true);
    should(schema.isValidValueTypeForColumn('ctime', new Date().getTime())).equal(false);
    should(schema.isValidValueTypeForColumn('ctime', '4')).equal(true);
    should(schema.isValidValueTypeForColumn('ctime', '1/1/2016 11:10')).equal(true);
    should(schema.isValidValueTypeForColumn('ctime', new Date())).equal(true);
    should(schema.isValidValueTypeForColumn('id', '125451')).equal(false);
    should(schema.isValidValueTypeForColumn('id', '3635cf47-9dc5-4bf9-8d08-b43a3252e5ee')).equal(true);
    should(schema.isValidValueTypeForColumn('tags', '1,2,3,4')).equal(false);
    should(schema.isValidValueTypeForColumn('tags', ['1','2','3','4'])).equal(true);
    done();
  });

  it('should handle composite keys correctly', (done) => {
    const newSchema = _.cloneDeep(BASE_SCHEMA);
    newSchema.key = [['name', 'email'], 'id'];
    let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);

    should(schema.partitionKey()).deepEqual(['name', 'email']);
    should(schema.clusteringKey()).equal('id');
    
    should(schema.isKeyColumn('name')).equal(true);
    should(schema.isKeyColumn('email')).equal(true);
    should(schema.isKeyColumn('id')).equal(true);

    newSchema.key = [['name', 'email'], 'id', 'ctime', 'utime'];
    schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
    should(schema.clusteringKey()).deepEqual(['id', 'ctime', 'utime']);

    newSchema.key = [['name', 'email']];
    schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
    should(schema.clusteringKey()).equal(false);
    
    done();
  });

  it('should provide correctly mixin with a Model', (done) => {
    done();
  });

  it('should identify counter type correctly', (done) => {
    const newSchema = _.cloneDeep(BASE_SCHEMA);
    newSchema.columns.id = 'counter';
    let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);

    should(schema.isCounterColumnFamily).equal(true);
    done();
  });

  describe('+ Invalid Definition', () => {
    it('should throw an error if invalid field is found in the defintion', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.invalid = {};
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidSchemaDefinitionKey');
        should(err.message).equal('Unknown schema definition key: invalid.');
        done();
      }
    });

    it('should throw an error if key field does not exist', (done) => {
      try {
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), _.omit(BASE_SCHEMA, 'key'));
      }
      catch(err) {
        should(err.errorType).equal('MissingDefinition');
        should(err.message).equal('Schema must define a key.');
        done();
      }
    });

    it('should throw an error if columns does not exist', (done) => {
      try {
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), _.omit(BASE_SCHEMA, 'columns'));
      }
      catch(err) {
        should(err.errorType).equal('MissingDefinition');
        should(err.message).equal('Schema must define columns.');
        done();
      }
    });

    it('should throw an error if column is not a string', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.columns.age = 4;
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.message).equal('Invalid object');
        done();
      }
    });

    it('should throw an error if column `type` does not exist', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        delete newSchema.columns.email.type;
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidTypeDefinition');
        should(err.message).equal('Type must be defined in column: email schema.');
        done();
      }
    });

    it('should throw an error if column `type` is not a string', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.columns.email.type = 4; // not a string
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidTypeDefinition');
        should(err.message).equal('Type: 4 should be a string in column: email schema.');
        done();
      }
    });

    it('should throw an error if column has invalid type', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.columns.email.type = 'notvalid'; // not a string
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidTypeDefinition');
        should(err.message).equal('Invalid type: notvalid in column: email schema.');
        done();
      }
    });

    it('should throw an error if column get/set is not a function', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.columns.email.get = 'notvalid'; // not a function
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidGetterSetterDefinition');
        should(err.message).equal('Setter / getters should be functions in column: email schema.');
        done();
      }
    });

    it('should throw an error if column alias is not a string', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.columns.email.alias = 4; // not a function
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidAliasDefinition');
        should(err.message).equal('Alias should be a string in column: email schema.');
        done();
      }
    });

    it('should throw an error if column alias is already used for another column', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.columns.other = {
          type: 'text',
          alias: 'emailAddress'
        };
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidAliasDefinition');
        should(err.message).equal('Alias conflicts with another alias or column name in column: other schema.');
        done();
      }
    });

    it('should throw an error if column object has invalid property', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.columns.email.badField = true;
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidColumnDefinitionKey');
        should(err.message).equal('Invalid column definition key: badField in column: email schema.');
        done();
      }
    });
  });

  describe('+ Invalid Key', () => {
    it('should throw an error if key contains an invalid column', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.key = ['id', 'invalid'];
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidKeyDefinition');
        should(err.message).equal('Key refers to invalid column.');
        done();
      }
    });

    it('should throw an error if key contains an invalid column in a composite key', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.key = [['id', 'name'], 'invalid'];
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidKeyDefinition');
        should(err.message).equal('Key refers to invalid column.');
        done();
      }
    });

    it('should throw an error if key contains a non-string', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.key = ['id', 4];
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidType');
        should(err.message).equal('Type should be a string.');
        done();
      }
    });

    it('should throw an error if composite part of key appears at the end', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.key = ['name', ['id', 'ctime']];
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidKeyDefinition');
        should(err.message).equal('Composite key can only appear at beginning of key definition.');
        done();
      }
    });
  });

  describe(' + Invalid WITH', () => {
    it('should throw an error if with is not a valid object', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.with = 4;
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.message).equal('Invalid object');
        done();
      }
    });

    it('should throw an error if with contains an invalid property', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.with = { invalid: true };
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidWithDefinition');
        should(err.message).equal('Invalid with property: invalid.');
        done();
      }
    });

    it('should throw an error if $clustering_order_by property is invalid', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.with = { $compact_storage: true, $clustering_order_by: ['invalid'] };
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidWithDefinition');
        should(err.message).equal('Invalid with clustering order: invalid.');
        done();
      }
    });

    it('should throw an error if $clustering_order_by property is valid but no clustering key provided', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.key = [['id', 'name']];
        newSchema.with = { $compact_storage: true, $clustering_order_by: {name: '$asc'} };
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidWithDefinition');
        should(err.message).equal('Invalid with clustering column: name.');
        done();
      }
    });

    it('should throw an error if $clustering_order_by property is valid but clustering key does not contain column', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.key = [['id', 'ctime'], 'email', 'utime'];
        newSchema.with = { $compact_storage: true, $clustering_order_by: {name: '$asc'} };
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidWithDefinition');
        should(err.message).equal('Invalid with clustering column: name.');
        done();
      }
    });

    it('should throw an error if $clustering_order_by property is valid but clustering key does is not equal to the column', (done) => {
      try {
        const newSchema = _.cloneDeep(BASE_SCHEMA);
        newSchema.key = [['id', 'ctime'], 'email'];
        newSchema.with = { $compact_storage: true, $clustering_order_by: {name: '$asc'} };
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);
      }
      catch(err) {
        should(err.errorType).equal('InvalidWithDefinition');
        should(err.message).equal('Invalid with clustering column: name.');
        done();
      }
    });
  });
});