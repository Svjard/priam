/*globals describe, beforeEach, afterEach, it*/
import sinon        from 'sinon';
import should       from 'should';
import uncapitalise from '../../../src/orm/user-defined-types';

describe('ORM :: User-defined Types', () => {
  let sandbox;

  beforeEach(() => {
    sandbox = sinon.sandbox.create();
  });

  afterEach(() => {
    sandbox.restore();
  });

  describe('Test 1', () => {
    it('does nothing', (done) => {
      should(1).not.equal(undefined);
      done();
    });
  });
});