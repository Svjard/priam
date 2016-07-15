/*globals describe, beforeEach, afterEach, it*/
import sinon from 'sinon';
import should from 'should';
import cassandra from 'cassandra-driver';
import chalk from 'chalk';
import Orm from '../src';
import Model from '../src/model';
import { ErrorHandler, errors } from '../src/errors';

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
  key: [['email', 'name'], 'id']
};

class TestModel extends Model {
  static schema() {
    return BASE_SCHEMA;
  }
}

describe('ORM :: Model', () => {
  let sandbox;
  let orm;

  before(() => {
    orm = new Orm({ connection: { keyspace: 'test' } });
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

  it('should correctly create the new model', (done) => {
    TestModel.prototype.afterNew = sandbox.spy(TestModel.prototype, 'afterNew');
    let m = new TestModel();
    TestModel.prototype.afterNew.calledOnce.should.be.true();
    done();
  });

  it('should correctly register a new model and skip creation of table', (done) => {
    orm.addModel('test', TestModel, { model: { table: { ensureExists: false } } }).then(() => {
      should(1).equal(1);
      done();
    })
  });

  it('should correctly register a new model and create table', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly handle options-callbacks-skipAll', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly handle options-callbacks-skipAfterNew', (done) => {
    TestModel.prototype.afterNew = sandbox.spy(TestModel.prototype, 'afterNew');
    let m = new TestModel(null, { skipAfterNewHook: true });
    TestModel.prototype.afterNew.calledOnce.should.be.false();
    done();
  });

  it('should correctly handle options-callbacks-skipBeforeCreate', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly handle options-callbacks-skipAfterCreate', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly handle options-callbacks-skipBeforeValidate', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly handle options-callbacks-skipAfterValidate', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly handle options-callbacks-skipBeforeSave', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly handle options-callbacks-skipAfterSave', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly validate the model', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly perform change detection on model\'s fields', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly add methods for list type', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly add methods for set type', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly add methods for map type', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly add methods for counter type', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly allow for model inheritance', (done) => {
    should(1).equal(1);
    done();
  });

  // Querying
});