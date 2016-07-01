// Modules
import Orm from '../index';

/**
 * Gets string representation of a javascript date object.
 *
 * @param {Date} date Javascript date object to convert 
 * @returns {string}
 * @module bucketing
 */
export function fromDate(date) {
  return date.getUTCFullYear().toString() + '/' + (date.getUTCMonth() + 1).toString();
}

/**
 * Gets string representation of a TimeUUid type.
 *
 * @param {TimeUuid} timeUUID TimeUuid cassandra type
 * @returns {string}
 * @module bucketing
 */
export function fromTimeUUID(timeUUID) {
  return fromDate(Orm.getDateFromTimeUUID(timeUUID));
}

/**
 * Gets string representation of a current timestamp.
 *
 * @returns {string}
 * @module bucketing
 */
export function now() {
  return fromDate(new Date());
}

/**
 * Gets string representation of a current timestamp plus an offset.
 *
 * @param {number} offset Offset in milliseconds
 * @returns {string}
 * @module bucketing
 */  
export function nowOffset(offset) {
  return offset(now(), offset);
}
  
/**
 * Gets string representation of a timestamp plus an offset.
 *
 * @param {string} bucket Timestamp as represented by a string
 * @param {number} offset Offset in milliseconds
 * @returns {string}
 * @module bucketing
 */
export function offset(bucket, offset) {
  let parts = bucket.split('/');
  parts[0] = parseInt(parts[0]);
  parts[1] = parseInt(parts[1]);
  if (offset > 0) {
    offset = offset + parts[1] - 1;
    const years = Math.floor(offset / 12);
    parts[0] += years;
    parts[1] = offset % 12 + 1;
  }
  else if (offset < 0) {
    offset = offset + parts[1] - 1;
    const years = Math.ceil(offset / -12);
    parts[0] -= years;
    offset = offset % 12;
    parts[1] = offset < 0 ? offset + 13 : offset + 1;
  }

  return parts.join('/');
}
