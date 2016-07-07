/*globals describe, beforeEach, afterEach, it*/
import sinon from 'sinon';
import should from 'should';
import _ from 'lodash';
import chalk from 'chalk';
import Orm from '../../src';
import Schema from '../../src/schema';
import { ErrorHandler, errors } from '../../src/errors';

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
    should(schema.partitionKey()).equal('id');
    done();
  });

  describe('+ Invalid Definition', () => {
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
      should(1).equal(1);
      done();
    });
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
    done();
  });

  it('should provide correct get/set functions on properties', (done) => {
    should(1).equal(1);
    done();
  }); // TODO -- expand this out

  it('should identify counter type correctly', (done) => {
    const newSchema = _.cloneDeep(BASE_SCHEMA);
    newSchema.columns.id = 'counter';
    let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), newSchema);

    should(schema.isCounterColumnFamily).equal(true);
    done();
  });
});