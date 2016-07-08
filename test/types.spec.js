/*globals describe, beforeEach, afterEach, it*/
import sinon from 'sinon';
import should from 'should';
import cassandra from 'cassandra-driver';
import chalk from 'chalk';
import * as types from '../src/types';
import { ErrorHandler, errors } from '../src/errors';

const cassandraTypes = [
  'timestamp',
  'inet',
  'int',
  'double',
  'float',
  'counter',
  'bigint',
  'varint',
  'set<uuid>',
  'ascii',
  'boolean',
  'decimal',
  'timeuuid',
  'varchar',
  'text',
  'map<text,int>',
  'set<map<text,map<text,int>>>',
  'set<frozen<map<text,frozen<map<text,int>>>>>',
  'list<frozen<tuple<text,int,text>>>',
  'frozen<address>',
  'list<frozen<address>>'
];

describe('ORM :: Types', () => {
  let sandbox;

  beforeEach(() => {
    sandbox = sinon.sandbox.create();
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('should remove whitespace from type name', (done) => {
    cassandraTypes.forEach((e) => {
      should(types.sanitize(e)).equal(e);
    });

    done();
  });
});