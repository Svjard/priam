// Libraries
import cassandra from 'cassandra-driver';
import _ from 'lodash';
// Modules
import { errors } from './errors';
import * as helpers from './helpers';

const cassandraTypes = cassandra.types;

function extract(type) {
  let i = type.indexOf('<');
  if (i < 0) {
    return {
      keyword: type,
      contents: null
    }
  }
  else {
    return { 
      keyword: type.substring(0, i),
      contents: type.substring(i + 1, type.length - 1)
    }
  }
}

function split(type) {
  let level = 0;
  let part = '';
  let parts = [];
  
  const length = type.length;
  for(let i = 0; i < length; i++) {
    const c = type[i];
    if (c === '<') {
      level++;
    }
    else if (c === '>') {
      level--;
    }
    else if (c === ',' && level == 0) {
      parts.push(part);
      part = '';
      continue;
    }
    part += c;
  }
  parts.push(part);
  
  return parts;
}

export const KEYWORDS = {
  // primitives
  ascii     : { validator: _.isString,            dbValidator: 'org.apache.cassandra.db.marshal.AsciiType',         size: 0 },
  bigint    : { validator: helpers.isInteger,     dbValidator: 'org.apache.cassandra.db.marshal.LongType',          size: 0 },
  blob      : { validator: helpers.isAnything,    dbValidator: 'org.apache.cassandra.db.marshal.BytesType',         size: 0 },
  boolean   : { validator: _.isBoolean,           dbValidator: 'org.apache.cassandra.db.marshal.BooleanType',       size: 0 },
  counter   : { validator: helpers.isInteger,     dbValidator: 'org.apache.cassandra.db.marshal.CounterColumnType', size: 0 },
  decimal   : { validator: _.isNumber,            dbValidator: 'org.apache.cassandra.db.marshal.DecimalType',       size: 0 },
  double    : { validator: _.isNumber,            dbValidator: 'org.apache.cassandra.db.marshal.DoubleType',        size: 0 },
  float     : { validator: _.isNumber,            dbValidator: 'org.apache.cassandra.db.marshal.FloatType',         size: 0 },
  inet      : { validator: helpers.isInet,        dbValidator: 'org.apache.cassandra.db.marshal.InetAddressType',   size: 0 },
  int       : { validator: helpers.isInteger,     dbValidator: 'org.apache.cassandra.db.marshal.Int32Type',         size: 0 },
  text      : { validator: _.isString,            dbValidator: 'org.apache.cassandra.db.marshal.UTF8Type',          size: 0 },
  timestamp : { validator: helpers.isDateTime,    dbValidator: 'org.apache.cassandra.db.marshal.TimestampType',     size: 0 },
  timeuuid  : { validator: helpers.isUUID,        dbValidator: 'org.apache.cassandra.db.marshal.TimeUUIDType',      size: 0 },
  uuid      : { validator: helpers.isUUID,        dbValidator: 'org.apache.cassandra.db.marshal.UUIDType',          size: 0 },
  varchar   : { validator: _.isString,            dbValidator: 'org.apache.cassandra.db.marshal.UTF8Type',          size: 0 },
  varint    : { validator: helpers.isInteger,     dbValidator: 'org.apache.cassandra.db.marshal.IntegerType',       size: 0 },
  
  // collections
  list      : { validator: _.isArray,             dbValidator: 'org.apache.cassandra.db.marshal.ListType',          size: 1 },
  set       : { validator: _.isArray,             dbValidator: 'org.apache.cassandra.db.marshal.SetType',           size: 1 },
  map       : { validator: helpers.isPlainObject, dbValidator: 'org.apache.cassandra.db.marshal.MapType',           size: 2 },
  
  // tuple
  tuple     : { validator: helpers.isTuple,       dbValidator: 'org.apache.cassandra.db.marshal.TupleType',         size: -1 },
  
  // frozen
  frozen    : {                                   dbValidator: 'org.apache.cassandra.db.marshal.FrozenType',        size: 1 }
};

export function sanitize(type) {
  return type.replace(/ /g, '');
}

export function isUserDefinedType(orm, type) {
  return !!orm.getUserDefinedType(type);
}

export function baseType(type) {
  while(true) {
    const e = extract(type);
    if (e.keyword === 'frozen') {
      type = e.contents;
    }
    else {
      return e.keyword;
    }
  }
}

export function mapKeyType(orm, mapType) {
  const e = extract(mapType);
  if (e.keyword !== 'map') {
    throw new errors.Types.InvalidArgument(i18n.t('errors.orm.general.invalidArgumentFormat'));
  }
  return baseType(orm, split(e.contents)[0]);
}

export function isStringType(orm, type) {
  const definition = KEYWORDS[type];
  return definition && (definition.dbValidator === 'org.apache.cassandra.db.marshal.AsciiType' || definition.dbValidator === 'org.apache.cassandra.db.marshal.UTF8Type');
}

export function isNumberType(orm, type) {
  const definition = KEYWORDS[type];
  return definition && (definition.dbValidator === 'org.apache.cassandra.db.marshal.CounterColumnType' || definition.dbValidator === 'org.apache.cassandra.db.marshal.DecimalType' || definition.dbValidator === 'org.apache.cassandra.db.marshal.DoubleType' || definition.dbValidator === 'org.apache.cassandra.db.marshal.FloatType' || definition.dbValidator === 'org.apache.cassandra.db.marshal.Int32Type' || definition.dbValidator === 'org.apache.cassandra.db.marshal.IntegerType');
}

export function isBooleanType(orm, type) {
  const definition = KEYWORDS[type];
  return definition && (definition.dbValidator === 'org.apache.cassandra.db.marshal.BooleanType');
}

export function isValidType(orm, type) {
  const e = extract(type);
  const definition = KEYWORDS[e.keyword];
  if (definition) {
    if (e.contents) {
      const parts = split(e.contents);
      if (definition.size !== -1 && definition.size !== parts.length) {
        return false;
      }
      else {
        const length = parts.length;
        for (let i = 0; i < length; i++) {
          if (!isValidType(orm, parts[i])) {
            return false;
          }
        }
        return true;
      }
    }
    else {
      return definition.size == 0;
    }
  }
  else {
    return isUserDefinedType(orm, type);
  }
}

export function isValidValueType(orm, type, value) {
  const e = extract(type);
  const definition = KEYWORDS[e.keyword];
  console.log('isValidValueType #1', type, value, e, definition);
  if (definition) {
    if (e.contents) {
      console.log('isValidValueType #2', e.contents);
      const parts = split(e.contents);
      if (e.keyword === 'frozen') {
        return isValidValueType(orm, parts[0], value);
      } else if (!definition.validator(value)) {
        return false;
      } else if (e.keyword === 'map') {
        for (let key in value) {
          if (!isValidValueType(orm, parts[0], key) || !isValidValueType(orm, parts[1], value[key])) {
            return false;
          }
        }
        return true;
      } else if (e.keyword === 'tuple') {
        const length = parts.length;
        for (let i = 0; i < length; i++) {
          if (!isValidValueType(orm, parts[i], value[i])) {
            return false;
          }
        }
        return true;
      } else {
        const length = value.length;
        for (let i = 0; i < length; i++) {
          if (!isValidValueType(orm, parts[0], value[i])) {
            return false;
          }
        }
        return true;
      }
    } else {
      console.log('isValidValueType #3', e, e.keyword, definition, definition.validator, value);
      return definition.validator(value);
    }
  } else if (isUserDefinedType(orm, type)) {
    return orm.getUserDefinedType(type).isValidValueTypeForSelf(value);
  } else {
    return false;
  }
}

export function dbValidator(orm, type, excludeFrozen) {
  let validator = '';
  let keyword = '';
  let frozen = {};
  let level = 0;
  _.each(type, (c, index) => {
    if (c === '<' || c === '>' || c === ',') {
      if (c === '<') {
        level++;
      }
      else if (c === '>') {
        if (frozen[level] && frozen[level] > 0) {
          frozen[level] -= 1;
          c = '';
        }
        level--;
      }
      
      if (keyword === 'frozen') {
        if (!frozen[level]) {
          frozen[level] = 1;
        }
        else {
          frozen[level] += 1;
        }
        keyword = '';
      }
      else {
        if (keyword.length > 0) {
          if (isUserDefinedType(orm, keyword)) {
            validator += orm.getUserDefinedType(keyword).dbValidator();
          }
          else if (keyword === 'tuple') {
            validator += KEYWORDS[keyword].dbValidator;
          }
          else {
            
            if (!excludeFrozen && c === '<' && frozen[level - 1] && frozen[level - 1] > 0) {
              frozen[level - 1] -= 1;
              validator += KEYWORDS['frozen'].dbValidator + '<';
            }
            
            validator += KEYWORDS[keyword].dbValidator;
          }
          keyword = '';
        }
        validator += c;
      }
    }
    else {
      keyword += c;
    }
  });

  if (keyword.length > 0) {
    if (isUserDefinedType(orm, keyword)) {
      validator += orm.getUserDefinedType(keyword).dbValidator();
    }
    else {
      validator += KEYWORDS[keyword].dbValidator;
    }
  }
  validator = validator.replace(/</g, '(');
  validator = validator.replace(/>/g, ')');
  return validator;
}

export function formatValueType(orm, type, value) {
  const e = extract(type);
  const definition = KEYWORDS[e.keyword];
  if (definition) {
    if (e.contents) {
      const parts = split(e.contents);
      if (e.keyword === 'frozen') {
        return formatValueType(orm, parts[0], value);
      }
      else if (e.keyword === 'map') {
        _.each(value, (v, key) => {
          key = formatValueType(orm, parts[0], key);
          value[key] = formatValueType(orm, parts[1], v);
        });
        return value;
      }
      else if (e.keyword === 'tuple') {
        value = _.map(parts, (part, index) => {
          return formatValueType(orm, part, value[index]);
        });
        return cassandraTypes.Tuple.fromArray(value);
      }
      else {
        value = _.map(value, (v, index) => {
          return formatValueType(orm, parts[0], v);
        });
        return value;
      }
    }
    else {
      return value;
    }
  }
  else if (isUserDefinedType(orm, type)) {
    return orm.getUserDefinedType(type).formatValueTypeForSelf(value);
  }
  else {
    return false;
  }
}

export function castValue(orm, value) {
  // cassandra tuple
  if (value instanceof cassandraTypes.Tuple) {
    value = value.values();
  }
  
  // cassandra types
  if (value instanceof cassandraTypes.BigDecimal) {
    return value.toNumber();
  }
  else if (value instanceof cassandraTypes.InetAddress) {
    return value.toString();
  }
  else if (value instanceof cassandraTypes.Integer) {
    return value.toNumber();
  }
  else if (value instanceof cassandraTypes.LocalDate) {
    return value.toString();
  }
  else if (value instanceof cassandraTypes.LocalTime) {
    return value.toString();
  }
  else if (value instanceof cassandraTypes.TimeUuid) {
    return value.toString();
  }
  else if (value instanceof cassandraTypes.Uuid) {
    return value.toString();
  }
  
  // array
  if (_.isArray(value)) {
    _.each(value, function(v, index) {
      value[index] = castValue(orm, v);
    });
    return value;
  }
  
  // hash
  else if (helpers.isPlainObject(value)) {
    _.each(value, function(v, key) {
      value[key] = castValue(orm, v);
    });
    return value;
  }
  
  return value;
}
