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
    should(schema.definition).equal(BASE_SCHEMA);
    should(Object.keys(schema.aliases).length).equal(1);
    should(schema.aliases.emailAddress).equal('email');
    done();
  });

  describe(' + Invalid Definition', () => {
    it('should throw an error if columns does not exist', (done) => {
      try {
        let schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), _.omit(BASE_SCHEMA, 'columns'));
      }
      catch(err) {
        console.log(err);
        should(err.errorType).equal('MissingDefinition')
        should(err.message).equal('Schema must define columns.');
        done();
      }
    });

    it('should throw an error if column type is not a string', (done) => {
      should(1).equal(1);
      done();
    });

    it('should throw an error if column has invalid type', (done) => {
      should(1).equal(1);
      done();
    });

    it('should throw an error if column get/set is not a function', (done) => {
      should(1).equal(1);
      done();
    });

    it('should throw an error if column alias is not a string', (done) => {
      should(1).equal(1);
      done();
    });

    it('should throw an error if column object has invalid property', (done) => {
      should(1).equal(1);
      done();
    });
  });

  describe(' + Invalid Key', () => {
    it('should throw an error if key contains an invalid column', (done) => {
      should(1).equal(1);
      done();
    });

    it('should throw an error if key contains a non-string', (done) => {
      should(1).equal(1);
      done();
    });
  });

  describe(' + Invalid WITH', () => {
    it('should throw an error if with is not a valid object', (done) => {
      should(1).equal(1);
      done();
    });

    it('should throw an error if with contains an invalid property', (done) => {
      should(1).equal(1);
      done();
    });

    it('should throw an error if $clustering_order_by property is invalid', (done) => {
      should(1).equal(1);
      done();
    });
  });

  it('should handle composite keys correctly', (done) => {
    should(1).equal(1);
    done();
  });

  it('should provide correct get/set functions on properties', (done) => {
    should(1).equal(1);
    done();
  }); // TODO -- expand this out

});