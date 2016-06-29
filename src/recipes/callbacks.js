// Modules
import Orm from '../index';

/**
 * Applies a new UUID to a given column in the table.
 *
 * @param {string} column Name of the column in the table. 
 */
export function setUUID(column, instance) {
  return ((column, instance) => {
    return () => {
      instance.set(column, Orm.generateUUID());
    }
  })(column, instance);
}
  
/**
 * Applies a new time-base UUID to a given column in the table.
 *
 * @param {string} column Name of the column in the table. 
 */
export function setTimeUUID(column, instance) {
  return ((column, instance) => {
    return () => {
      instance.set(column, Orm.generateTimeUUID());
    }
  })(column, instance);
}

/**
 * Applies a new timestamp to a given column in the table.
 *
 * @param {string} column Name of the column in the table. 
 */
export function setTimestampToNow(column, instance) {
  return ((column, instance) => {
    return () => {
      instance.set(column, Orm.nowToTimestamp());
    }
  })(column, instance);
}
