/*globals describe, beforeEach, afterEach, it*/
import sinon from 'sinon';
import should from 'should';
import cassandra from 'cassandra-driver';
import chalk from 'chalk';
import Keyspace from '../../src/keyspace';
import { SimpleStrategy } from '../../src/replication-strategies';
import { ErrorHandler, errors } from '../../src/errors';

const currentEnv = process.env.NODE_ENV;

describe('ORM :: Keyspace', () => {
  let client;
  let keyspace;
  let sandbox;

  before((done) => {
    client = new cassandra.Client({ contactPoints: ['127.0.0.1'], keyspace: 'system'});
    keyspace = new Keyspace(client, 'test', new SimpleStrategy(1), true, {}); // replication, durableWrites, options
    client.execute('DROP KEYSPACE IF EXISTS test', [], function(err, result) {
      done();
    });
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

  it('should correctly honor the ensureExists - run flag', (done) => {
    keyspace.ensureExists({ run: false }).then(result => {
      ErrorHandler.logWarn.calledOnce.should.be.true();
      ErrorHandler.logWarn.calledWith(
        chalk.yellow('\nEnsure keyspace skipped: test.'), '\n');
      done();
    });
  });

  it('should reject with SelectSchemaError error if unable to query system schema', (done) => {
    sinon.stub(keyspace, 'execute').returns(Promise.resolve({ rows: undefined }));
    keyspace.ensureExists({ run: true }).catch(err => {
      should(err.errorType).equal('SelectSchemaError');
      should(err.message).equal('Select schema returned no result or no rows.');
      keyspace.execute.restore();
      
      done();
    });
  });

  it('should correctly create the keyspace if it does not exist', (done) => {
    keyspace.ensureExists({ run: true }).then(result => {
      ErrorHandler.logWarn.calledOnce.should.be.true();
      ErrorHandler.logWarn.calledWith(
        chalk.yellow('\nCreating keyspace: test.'), '\n');
      
      keyspace.selectSchema().then(result => {
        should(result).not.equal(undefined);
        should(result.rows).not.equal(undefined);
        should(result.rows.length).equal(1);
        done();
      }).catch(err => {
      console.error(err);
    });
    }).catch(err => {
      console.error(err);
    });
  });

  it('should not alter the keyspace if alter flag not set', (done) => {
    keyspace.durableWrites = false;

    keyspace.ensureExists({ run: true, alter: false }).then(result => {
      ErrorHandler.logWarn.calledOnce.should.be.true();
      ErrorHandler.logWarn.calledWith(
        chalk.yellow('\nDifferent durable writes value found for existing keyspace: test.'), '\n');
      
      keyspace.selectSchema().then(result => {
        should(result).not.equal(undefined);
        should(result.rows).not.equal(undefined);
        should(result.rows.length).equal(1);
        should(result.rows[0].durable_writes).equal(true);

        done();
      });
    });
  });

  it('should correctly alter the keyspace if alter flag set and if durable_writes has changed', (done) => {
    keyspace.durableWrites = false;

    keyspace.ensureExists({ run: true, alter: true }).then(result => {
      keyspace.selectSchema().then(result => {
        should(result).not.equal(undefined);
        should(result.rows).not.equal(undefined);
        should(result.rows.length).equal(1);
        should(result.rows[0].durable_writes).equal(false);

        done();
      });
    });
  });

  it('should correctly alter the keyspace if alter flag set and if replication strategies has changed', (done) => {
    keyspace.replication = new SimpleStrategy(11);

    keyspace.ensureExists({ run: true, alter: true }).then(result => {
      keyspace.selectSchema().then(result => {
        should(result).not.equal(undefined);
        should(result.rows).not.equal(undefined);
        should(result.rows.length).equal(1);
        should(result.rows[0].replication.class).equal(keyspace.replication.class);
        should(result.rows[0].replication.replication_factor).equal('11');

        done();
      });
    });
  });

  it('should not execute an alter command if alter flag set but no changes detected', (done) => {
    keyspace.alter = sandbox.spy(keyspace, 'alter');
    keyspace.ensureExists({ run: true, alter: true }).then(result => {
      keyspace.alter.calledOnce.should.be.false();
      done();
    });
  });

  it('should correct drop a keyspace', (done) => {
    keyspace.drop({ ifExists: true }).then(result => {
      keyspace.selectSchema().then(result => {
        should(result).not.equal(undefined);
        should(result.rows).not.equal(undefined);
        should(result.rows.length).equal(0);

        done();
      });
    });
  });
});