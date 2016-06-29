/*globals describe, beforeEach, afterEach, it*/
import sinon        from 'sinon';
import should       from 'should';
import * as helpers from '../../src/helpers';

describe('ORM :: Helpers', () => {
  let sandbox;

  beforeEach(() => {
    sandbox = sinon.sandbox.create();
  });

  afterEach(() => {
    sandbox.restore();
  });

  describe('Basic Type Methods', () => {
    it('should correctly detect plain objects', (done) => {
      should(helpers.isPlainObject('')).equal(false);
      should(helpers.isPlainObject(null)).equal(false);
      should(helpers.isPlainObject(undefined)).equal(false);
      should(helpers.isPlainObject([])).equal(false);
      should(helpers.isPlainObject({ 'foo': {} })).equal(true);
      
      done();
    });

    it('should correctly detect integers', (done) => {
      should(helpers.isInteger('5')).equal(true);
      should(helpers.isInteger(5)).equal(true);

      done();
    });

    it('should correctly detect datetime objects', (done) => {
      should(helpers.isDateTime('3/2/2013')).equal(true);
      should(helpers.isDateTime('02/09/2013')).equal(true);
      should(helpers.isDateTime(new Date())).equal(true);

      done();
    });

    it('should correctly detect anything', (done) => {
      should(helpers.isAnything()).equal(true);

      done();
    });

    it('should correctly detect UUIDs', (done) => {
      should(helpers.isUUID('foo')).equal(false);
      should(helpers.isUUID('91bf651a-3a44-11e6-ac61-9e71128cae77')).equal(true); // v1
      should(helpers.isUUID('40ee4570-1de5-472b-9fe6-79c134d0012e')).equal(true); // v4
      should(helpers.isUUID('40ee4570-1de5-472b-9fe6-79c134d0012e  ')).equal(false); // v4

      done();
    });

    it('should correctly detect internet addresses', (done) => {
      should(helpers.isInet('211.125.220.207')).equal(true);
      should(helpers.isInet('148.49.171.4')).equal(true);
      should(helpers.isInet('148.49.171.4  ')).equal(false);
      should(helpers.isInet('fd69:5366:1f15:9bd9:0:0:0:0')).equal(true);
      should(helpers.isInet('fd69:5366:1f15:9bd9:ffff:ffff:ffff:ffff')).equal(true);
      should(helpers.isInet('fd69:5366:1f15:9bd9::/64')).equal(false);

      done();
    });

    it('should correctly detect tuples', (done) => {
      should(helpers.isTuple('2,3,4')).equal(false);
      should(helpers.isTuple([1, '4', {foo: 'bar'}])).equal(true);
      should(helpers.isTuple(null)).equal(false);
      should(helpers.isTuple([null, 4, 5])).equal(true);
      should(helpers.isTuple([4, 5])).equal(false);

      done();
    });
  });
});