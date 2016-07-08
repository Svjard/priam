/*globals describe, beforeEach, afterEach, it*/
import sinon        from 'sinon';
import should       from 'should';
import * as sanitzers from '../src/recipes/sanitizers';

describe('ORM :: Recipes', () => {
  let sandbox;

  beforeEach(() => {
    sandbox = sinon.sandbox.create();
  });

  afterEach(() => {
    sandbox.restore();
  });

  describe('Sanitzers', () => {
    it('should sanitize email addresses correctly', (done) => {
      should(sanitzers.email('abc@foo.com')).equal('abc@foo.com');
      should(sanitzers.email('aaa ')).equal('aaa');
      should(sanitzers.email('a123 foo.com')).equal('a123 foo.com');
      should(sanitzers.email('aBc@FOO.com')).equal('abc@foo.com');
      done();
    });

    it('should map sanitzers against array of values', (done) => {
      const m = sanitzers.map(sanitzers.email);

      should(sanitzers.email('abc@foo.com')).equal('abc@foo.com');
      should(m(['aBc@FOO.com'])).deepEqual(['abc@foo.com']);
      should(m(['aBc@FOO.com', 'aaa '])).deepEqual(['abc@foo.com', 'aaa']);
      done();
    });

    it('should sanitize to lowercase', (done) => {
      should(sanitzers.lowercase('AbCC')).equal('abcc');
      should(sanitzers.lowercase('null')).equal('null');
      done();
    });

    it('should sanitize by trimming whitespace', (done) => {
      should(sanitzers.trim('abc ')).equal('abc');
      should(sanitzers.trim('   abc')).equal('abc');
      done();
    });
  });

  describe('Callbacks', () => {
    it('should do nothing', (done) => {
      done();
    })
  });
});