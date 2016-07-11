/*globals describe, beforeEach, afterEach, it*/
import sinon from 'sinon';
import should from 'should';
import { Chance } from 'chance';
import cassandra from 'cassandra-driver';
import chalk from 'chalk';
import Orm from '../src';
import Query from '../src/query';
import { ErrorHandler, errors } from '../src/errors';

let chance = new Chance();

const currentEnv = process.env.NODE_ENV;
const cassandraTypes = cassandra.types;

describe('ORM :: Query', () => {
  let sandbox;
  let client;
  let dataset;

  before((done) => {
    client = new cassandra.Client({ contactPoints: ['127.0.0.1'], keyspace: 'system'});
    client.execute('CREATE KEYSPACE IF NOT EXISTS test', [], function(err, result) {
      // Generate our table
      let query = 'CREATE TABLE IF NOT EXISTS test.tbl1 (id1 uuid, id2 timeuuid, txt text, val int, street text, city text, state text, zip int, phones set<text>, currencies frozen<tuple<text, text>>, value decimal, other map<text, text>, PRIMARY KEY(id1, id2))';
      client.execute(query, [], function(err, result) {
        // Insert our fake data
        query = 'INSERT INTO test.tbl1 VALUES(?,?,?,?,?,?,?,?,?,?,?)';
        dataset = [];
        for (let i = 0; i < 100; i++) {
          dataset.push({
            query: query,
            params: [cassandraTypes.Uuid.random(), new cassandraTypes.TimeUuid.now(), chance.paragraph(), chance.integer(), chance.street(), chance.city(), chance.province(), chance.zip(), [chance.phone(), chance.phone()], new cassandraTypes.Tuple(chance.currency().code, chance.currency().code, chance.currency().code), chance.floating(), {foo: chance.word(), bar: chance.word()}]
          });
        }

        client.batch(dataset, { prepare: true }, function(err) {
          done();
        });
      });
    });
  });

  after((done) => {
    client.execute('DROP KEYSPACE IF EXISTS test', [], function(err, result) {
      done();
    });
  });

  beforeEach(() => {
    sandbox = sinon.sandbox.create();
    process.env.NODE_ENV = 'development';
  });

  afterEach(() => {
    sandbox.restore();
    process.env.NODE_ENV = currentEnv;  
  });

  it('should correctly find all rows with positive `val` and positive `value`', (done) => {
    done();
  });

  it('should correctly find the first row with a specific `txt`', (done) => {
    //dataset[0].params[2];
    done();
  });

  it('should correctly set the where conditions', (done) => {
    /*let query = new Query({}, {});
    query.where('abc', 1);
    should(query.where.abc).deepEqual({ '$eq': 1 });
    query.where(['foo', 'hello']);
    should(query.where.foo).deepEqual({ '$eq': 'hello' });
    query.where(['bar', { '$lt': 100 }]);
    should(query.where.bar).deepEqual({ '$lt': 100 });*/

    done();
  });

  it('should correctly set the action', (done) => {
    
    done();
  });

  it('should correctly set the orderby conditions', (done) => {
    //dataset[0].params[2];
    done();
  });

  it('should correctly set the select columns', (done) => {
    //dataset[0].params[2];
    done();
  });

});