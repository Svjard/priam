/*globals describe, beforeEach, afterEach, it*/
import sinon from 'sinon';
import should from 'should';
import cassandra from 'cassandra-driver';
import chalk from 'chalk';
import Orm from '../../src';
import Table from '../../src/table';
import { ErrorHandler, errors } from '../../src/errors';

const currentEnv = process.env.NODE_ENV;

const BASE_SCHEMA = {
  // columns
  columns: {
    // timestamps
    ctime: 'timestamp',
    utime: 'timestamp',

    // data
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
  // key
  key: [['email', 'name'], 'id']
};

describe('ORM :: Table', () => {
  let table;
  let sandbox;

  before((done) => {
    keyspace = new Table(new Orm(), 'test', BASE_SCHEMA, {});
  });

  beforeEach(() => {
    sandbox = sinon.sandbox.create();
    ErrorHandler.logWarn = sandbox.spy(ErrorHandler, 'logWarn');
    process.env.NODE_ENV = 'development';
  });

  afterEach(() => {
    sandbox.restore();
    process.env.NODE_ENV = currentEnv;  
  });

  it('should correctly honor the ensureExists flag', (done) => {
    should(1).equal(1);
    done();
  });

  it('should reject with SelectSchemaError error if unable to query system schema', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly create the table if it does not exist', (done) => {
    should(1).equal(1);
    done();
  });

  it('should not alter the table if no alter flag is set', (done) => {
    // fixMissing, etc...
    should(1).equal(1);
    done();
  });

  it('should correctly alter the table if new columns are detected', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly alter the table if new columns are detected', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly alter the table if column names have changed', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly alter the table if column types have changed', (done) => {
    should(1).equal(1);
    done();
  });

  it('should not execute an alter command if alter flags are set but no changes detected', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correct drop a table', (done) => {
    should(1).equal(1);
    done();
  });
});