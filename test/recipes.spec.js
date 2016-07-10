/*globals describe, beforeEach, afterEach, it*/
import sinon        from 'sinon';
import should       from 'should';
import * as sanitzers from '../src/recipes/sanitizers';
import * as validators from '../src/recipes/validators';

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

  describe('Validators', () => {
    it('should validate an email address correctly', (done) => {
      should(validators.email.validator('abc@foo.com')).equal(true);
      should(validators.email.validator('aaa ')).equal(false);
      should(validators.email.validator('a123 foo.com')).equal(false);
      should(validators.email.validator('aBc@FOO.com')).equal(true);
      should(validators.email.message('Email Address')).equal('Email Address must be a valid email address.');
      done();
    });

    it('should validate a password correctly', (done) => {
      should(validators.password.validator('password')).equal(false);
      should(validators.password.validator('abd')).equal(false);
      should(validators.password.validator('Mhks918')).equal(true);
      should(validators.password.validator('aBc.89!aERt')).equal(true);
      should(validators.password.validator('')).equal(false);
      should(validators.password.message('Password')).equal('Password must be at least 6 characters long and contain one number and one letter.');
      done();
    });

    it('should validate a required field', (done) => {
      should(validators.required.validator('password')).equal(true);
      should(validators.required.validator(null)).equal(false);
      should(validators.required.validator(undefined)).equal(false);
      should(validators.required.validator(false)).equal(false);
      should(validators.required.validator(NaN)).equal(false);
      should(validators.required.validator('')).equal(false);
      should(validators.required.message('Password')).equal('Password is required.');
      done();
    });

    it('should validate a value in an array', (done) => {
      let f = validators.isIn([1, 2, {}, 5, 'abc', [4,5]]);

      should(f.validator('password')).equal(false);
      should(f.validator(1)).equal(true);
      should(f.validator({})).equal(false);
      should(f.validator('abc')).equal(true);
      should(f.validator(null)).equal(false);
      should(f.validator([4,5])).equal(false);
      should(f.message('Tags')).equal('Tags must have one of these values: 1, 2, [object Object], 5, abc, 4,5.');
      done();
    });

    it('should validate a value in an object', (done) => {
      let f = validators.isIn({
        a: 1,
        b: 2,
        c: {},
        d: 5,
        e: 'abc',
        f: null
      });

      should(f.validator('password')).equal(false);
      should(f.validator('a')).equal(true);
      should(f.validator('c')).equal(true);
      should(f.validator('e')).equal(true);
      should(f.validator('f')).equal(false);
      should(f.message('Tags')).equal('Tags must have one of these values: 1, 2, [object Object], 5, abc, .');
      done();
    });

    it('should validate a value being present in at least one field', (done) => {
      // TODO -- requires instance of ORM with schema
      done();
    });

    it('should validate a string or array with a minimum length', (done) => {
      let f = validators.minLength(2);
      should(f.validator('c')).equal(false);
      should(f.validator('escs')).equal(true);
      should(f.validator('')).equal(false);
      should(f.validator([null,null])).equal(true);
      should(f.validator([])).equal(false);
      should(f.message('Tags')).equal('Tags is too short (minimum is 2 characters).');
      done();
    });

    it('should validate a string or array with a maximum length', (done) => {
      let f = validators.maxLength(2);
      should(f.validator('c')).equal(true);
      should(f.validator('escs')).equal(false);
      should(f.validator('')).equal(true);
      should(f.validator([null,null])).equal(true);
      should(f.validator([])).equal(true);
      should(f.validator([1,2,3])).equal(false);
      should(f.message('Tags')).equal('Tags is too long (maximum is 2 characters).');
      done();
    });

    it('should validate a number greater than or equal to another number', (done) => {
      let f = validators.greaterThanOrEqualTo(2);
      should(f.validator('c')).equal(false);
      should(f.validator('')).equal(false);
      should(f.validator(0)).equal(false);
      should(f.validator(6)).equal(true);
      should(f.validator(NaN)).equal(false);
      should(f.message('Age')).equal('Age is too small (minimum is 2).');
      done();
    });

    it('should validate a number strictly greater than another number', (done) => {
      let f = validators.greaterThan(2);
      should(f.validator('c')).equal(false);
      should(f.validator('')).equal(false);
      should(f.validator(2)).equal(false);
      should(f.validator(6)).equal(true);
      should(f.message('Age')).equal('Age is too small (must be greater than 2).');
      done();
    });

    it('should validate a number less than or equal to another number', (done) => {
      let f = validators.lessThanOrEqualTo(2);
      should(f.validator('c')).equal(false);
      should(f.validator('')).equal(false);
      should(f.validator(0)).equal(true);
      should(f.validator(6)).equal(false);
      should(f.validator(NaN)).equal(false);
      should(f.message('Age')).equal('Age is too big (maximum is 2).');
      done();
    });

    it('should validate a number strictly less than another number', (done) => {
      let f = validators.lessThan(2);
      should(f.validator('c')).equal(false);
      should(f.validator('')).equal(false);
      should(f.validator(2)).equal(false);
      should(f.validator(-9)).equal(true);
      should(f.message('Age')).equal('Age is too big (must be less than 2).');
      done();
    });

    it('should validate value if conditional is true', (done) => {
      let istrue = (v, orm) => {
        return true;
      }

      let f = validators.validateIf(istrue, validators.minLength(2));
      should(f.validator('cdw')).equal(true);
      should(f.validator('c', null)).equal(false);
      let msgs = f.message('Age');
      should(msgs.length).equal(1);
      should(msgs[0]).equal('Age is too short (minimum is 2 characters).');
      
      let isfalse = (v, orm) => {
        return false;
      };
      f = validators.validateIf(isfalse, validators.minLength(2));
      should(f.validator('c', null)).equal(true);

      msgs = f.message('Age');
      should(msgs.length).equal(0);
      done();
    });

    it('should validate a specific field within an object', (done) => {
      let f = validators.validateObjectFields('age', 'Age', validators.minLength(2));
      should(f.validator({ age: 'c' })).equal(false);
      let msgs = f.message('Age');
      should(msgs.field).equal('age');
      should(msgs.messages.length).equal(1);
      should(msgs.messages[0]).equal('Age is too short (minimum is 2 characters).');
      should(f.validator(null)).equal(false);
      should(f.validator({ age: 'csd' })).equal(true);
      done();
    });

    it('should validate a value against multiple validators', (done) => {
      let f = validators.validateMultiple(validators.isIn(['abc', 'def', 'xyz']), validators.minLength(2));
      should(f.validator('fred')).equal(false);
      should(f.validator('cd')).equal(false);
      
      let msgs = f.message('Age');
      should(msgs.length).equal(1);
      should(msgs[0]).equal('Age must have one of these values: abc, def, xyz.');
      
      should(f.validator('xyz')).equal(true);

      done();
    });
    
  });

  describe('Callbacks', () => {
    it('should do nothing', (done) => {
      done();
    })
  });
});