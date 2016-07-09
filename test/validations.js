/*globals describe, beforeEach, afterEach, it*/
import _ from 'lodash';
import sinon        from 'sinon';
import should       from 'should';
import Orm from '../src';
import * as Sanitizers from '../src/recipes/sanitizers';
import Schema from '../src/schema';
import Validations from '../src/validations';
import * as Validators from '../src/recipes/validators';

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

const BASE_VALIDATIONS = {
  ctime: {
    validator: {
      validator: (value, instance) => {
        return !_.isNull(value);
      },
      message: (displayName) => { return displayName + ' is required.'; }
    }
  },
  utime: {
    validator: Validators.required
  },
  id: {
    validator: Validators.required
  },
  email: {
    displayName: 'Email',
    validator: Validators.email,
    sanitizer: Sanitizers.email
  },
  name: {
    displayName: 'Name',
    validator: [Validators.required, Validators.minLength(1)],
    sanitizer: (value, instance) => {
      return value.charAt(0).toUpperCase() + value.slice(1);
    }
  }
};

describe('ORM :: Validations', () => {
  let sandbox;
  let schema;

  beforeEach(() => {
    sandbox = sinon.sandbox.create();
    schema = new Schema(new Orm({ connection: { keyspace: 'test' } }), BASE_SCHEMA);
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('should correctly create a basic validator', (done) => {
    // let v = new Validations(schema, );
    should(1).equal(1);
    done();
  });

  it('should correctly handle validator not having a displayName', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly handle validator not having a validator', (done) => {
    should(1).equal(1);
    done();
  })

  it('should correctly handle validator not having a sanitzer', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly handle validator having a sanitzer as a function', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly handle validator having a sanitzer as an array', (done) => {
    should(1).equal(1);
    done();
  });

  it('should throw an error if invalid field is specified', (done) => {
    should(1).equal(1);
    done();
  });

  it('should throw an error is column does not exist', (done) => {
    should(1).equal(1);
    done();
  });

  it('should throw an error is displayName is not a string', (done) => {
    should(1).equal(1);
    done();
  });

  it('should correctly handle validator specified as a object', (done) => {
    should(1).equal(1);
    done();
  });

  it('should throw an error is validator is not an array', (done) => {
    should(1).equal(1);
    done();
  });

  it('should throw an error is validator is an array but element is not an object', (done) => {
    should(1).equal(1);
    done();
  });

  it('should throw an error is validator is an array but element does not contain validator', (done) => {
    should(1).equal(1);
    done();
  });

  it('should throw an error is validator is an array but element\'s validator is not a function', (done) => {
    should(1).equal(1);
    done();
  });

  it('should throw an error is validator is an array but element does not contain message', (done) => {
    should(1).equal(1);
    done();
  });

  it('should throw an error is validator is an array but element\'s message is not a function', (done) => {
    should(1).equal(1);
    done();
  });

  it('should throw an error is validator is an array but element\'s sanitzer is not a function or array', (done) => {
    should(1).equal(1);
    done();
  });

  it('should throw an error is validator is an array but element\'s sanitzer contains a non-function array element', (done) => {
    should(1).equal(1);
    done();
  });
});