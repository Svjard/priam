// Modules
import Orm from '../index';

/**
 * @namespace recipes.callbacks
 */

/**
 * Applies a new UUID to a given column in the table.
 *
 * @param {string} column Name of the column in the table.
 * @memberOf recipes.callbacks
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
 * @memberOf recipes.callbacks
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
 * @memberOf recipes.callbacks
 */
export function setTimestampToNow(column, instance) {
  return ((column, instance) => {
    return () => {
      instance.set(column, Orm.nowToTimestamp());
    }
  })(column, instance);
}
