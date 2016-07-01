// Libraries
import _ from 'lodash';

/**
 * The supported replication strategories for Cassandra.
 * @module replication-strategies
 */

/**
 * Base class for all replication stragegies.
 *
 * @class
 * @abstract
 */
export class ReplicationStrategy {
  equals(strategy) { return true; }
  toCassandra() { return {}; }
}

/**
 * The simple strategy that uses basic replication across a keyspace.
 * @class
 * @see https://docs.datastax.com/en/cql/3.3/cql/cql_reference/create_keyspace_r.html
 */
export class SimpleStrategy extends ReplicationStrategy {
  constructor(replicationFactor) {
    super();

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
   */
  equals(strategy) {
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
   */
  toCassandra() {
    return { 'class' : this.class, 'replication_factor': this.replicationFactor };
  }
}

/**
 * The network strategy that uses replication across nodes in a per data center configuration
 * for the keyspace.
 * @class
 * @see https://docs.datastax.com/en/cql/3.3/cql/cql_reference/create_keyspace_r.html
 */
export class NetworkTopologyStrategy extends ReplicationStrategy {
  constructor(dataCenters) {
    super();

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
     */
    this.dataCenters = dataCenters;
  }

  getDataCenter(name) {
    return this.dataCenters[name];
  }

  setDataCenter(name, factor) {
    this.dataCenters[name] = factor;
  }

  /**
   * Determines if the strategy as stored in Cassandra is equal to the current
   * instance of the strategy.
   *
   * @param {Object} strategy The strategy object from the Cassandra database,
   *    {@link https://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_keyspace_r.html}
   * @return {boolean}
   */
  equals(strategy) {
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
   */
  toCassandra() {
    const cassandraObj = { 'class' : this.class };
    Object.key(this.dataCenters).forEach(n => {
      cassandraObj[n] = this.dataCenters[n];
    });

    return cassandraObj;
  }
}