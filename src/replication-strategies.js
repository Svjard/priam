// Libraries
import _ from 'lodash';
import check from 'check-types';

/**
 * Base class for all replication stragegies.
 *
 * @class
 * @abstract
 */
export class ReplicationStrategy {
  /**
   * @function equals
   * @memberOf ReplicationStrategy
   * @instance
   * @abstract
   */
  equals(strategy) { return true; }
  /**
   * @function toCassandra
   * @memberOf ReplicationStrategy
   * @instance
   * @abstract
   */
  toCassandra() { return {}; }
}

/**
 * The simple strategy that uses basic replication across a keyspace.
 * @class
 * @see {@link https://docs.datastax.com/en/cql/3.3/cql/cql_reference/create_keyspace_r.html}
 */
export class SimpleStrategy extends ReplicationStrategy {
  constructor(replicationFactor) {
    super();

    check.integer(replicationFactor) & check.positive(replicationFactor);

    /**
     * The number of replicas of data on multiple nodes.
     *
     * @type {string}
     * @name class
     * @readonly
     * @memberOf SimpleStrategy
     * @instance
     */
    Object.defineProperty(this, 'class', {
      enumerable: false,
      configurable: false,
      writable: false,
      value: 'org.apache.cassandra.locator.SimpleStrategy'
    });

    /**
     * The number of replicas of data on multiple nodes.
     *
     * @type {number}
     * @name replicationFactor
     * @memberOf SimpleStrategy
     * @instance
     */
    this.replicationFactor = replicationFactor;
  }
  
  /**
   * Determines if the strategy as stored in Cassandra is equal to the current
   * instance of the strategy.
   *
   * @param {Object} strategy The strategy object from the Cassandra database,
   *    {@link https://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_keyspace_r.html}
   * @return {boolean}
   * @function equals
   * @memberOf SimpleStrategy
   * @instance
   */
  equals(strategy) {
    check.map(strategy, {
      class: check.maybe.nonEmptyString,
      replication_factor: check.maybe.nonEmptyString
    });

    if (strategy.class !== this.class) {
      return false;
    }

    if (!strategy.replication_factor || !this.replicationFactor) {
      return false;
    }

    if (strategy.replication_factor.toString() !== this.replicationFactor.toString()) {
      return false;
    }

    return true;
  }

  /**
   * Converts the replication strategy into an object understandable by Cassandra in
   * a CREATE KEYSPACE or ALTER KEYSPACE query.
   *
   * @return {Object}
   * @function toCassandra
   * @memberOf SimpleStrategy
   * @instance
   */
  toCassandra() {
    return { 'class' : this.class, 'replication_factor': this.replicationFactor };
  }
}

/**
 * The network strategy that uses replication across nodes in a per data center configuration
 * for the keyspace.
 * @class
 * @see {@link https://docs.datastax.com/en/cql/3.3/cql/cql_reference/create_keyspace_r.html}
 */
export class NetworkTopologyStrategy extends ReplicationStrategy {
  constructor(dataCenters) {
    super();

    check.object(dataCenters);
    
    /**
     * The number of replicas of data on multiple nodes.
     *
     * @type {string}
     * @name class
     * @readonly
     * @memberOf NetworkTopologyStrategy
     * @instance
     */
    Object.defineProperty(this, 'class', {
      enumerable: false,
      configurable: false,
      writable: false,
      value: 'org.apache.cassandra.locator.NetworkTopologyStrategy'
    });
    
    /**
     * The map of datacenter names to the number of replicas of data on each
     * node in the data center.
     *
     * @type {Object<string, number>}
     * @name dataCenters
     * @memberOf NetworkTopologyStrategy
     * @instance
     */
    this.dataCenters = dataCenters;
  }
  
  /**
   * Gets the replication factor for a given data center in the strategy.
   *
   * @param {string} name The name of the data center
   * @return {number}
   * @function getDataCenter
   * @memberOf NetworkTopologyStrategy
   * @instance
   */
  getDataCenter(name) {
    check.string(name);
    
    return this.dataCenters[name];
  }

  /**
   * Sets the replication factor for a given data center in the strategy.
   *
   * @param {string} name The name of the data center
   * @param {number} replicationFactor The replication factor
   * @function setDataCenter
   * @memberOf NetworkTopologyStrategy
   * @instance
   */
  setDataCenter(name, replicationFactor) {
    check.nonEmptyString(name);
    check.integer(replicationFactor) & check.positive(replicationFactor);

    this.dataCenters[name] = factor;
  }

  /**
   * Determines if the strategy as stored in Cassandra is equal to the current
   * instance of the strategy.
   *
   * @param {Object} strategy The strategy object from the Cassandra database,
   *    {@link https://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_keyspace_r.html}
   * @return {boolean}
   * @function equals
   * @memberOf NetworkTopologyStrategy
   * @instance
   */
  equals(strategy) {
    check.map(strategy, {
      class: check.maybe.nonEmptyString,
      dataCenters: check.maybe.object
    });

    if (strategy.class !== this.class) {
      return false;
    }

    const keys = Object.keys(this.dataCenters);
    for (let i = 0; i < keys.length; i++) {
      if (this.dataCenters[keys[i]] || !strategy[keys[i]]) {
        return false;
      }

      if (this.dataCenters[keys[i]].toString() !== !strategy[keys[i]].toString()) {
        return false;
      }
    }

    return true;
  }

  /**
   * Converts the replication strategy into an object understandable by Cassandra in
   * a CREATE KEYSPACE or ALTER KEYSPACE query.
   *
   * @return {Object}
   * @function toCassandra
   * @memberOf NetworkTopologyStrategy
   * @instance
   */
  toCassandra() {
    const cassandraObj = { 'class' : this.class };
    Object.key(this.dataCenters).forEach(n => {
      cassandraObj[n] = this.dataCenters[n];
    });

    return cassandraObj;
  }
}
