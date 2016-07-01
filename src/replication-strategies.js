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
class ReplicationStrategy {
  constructor() {
    this.class = '';
  }
}

/**
 * The simple strategy that uses basic replication across a keyspace.
 * @class
 * @see https://docs.datastax.com/en/cql/3.3/cql/cql_reference/create_keyspace_r.html
 */
export class SimpleStrategy extends ReplicationStrategy {
  constructor(replicationFactor) {
    /**
     * The name of the replication class, must be the fully qualified name
     *
     * @type string
     */
    this.class = 'org.apache.cassandra.locator.SimpleStrategy';
    this.replicationFactor = replicationFactor;
  }

  get replicationFactor() { return this.replicationFactor; }

  set replicationFactor(replicationFactor) { this.replicationFactor = replicationFactor; }

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
    this.class = 'org.apache.cassandra.locator.NetworkTopologyStrategy';
    this.dataCenters = dataCenters;
  }

  get dataCenters() { return this.dataCenters; }

  set dataCenters(replicationFactor) { this.dataCenters = dataCenters; }

  getDataCenter(name) {
    return this.dataCenters[name];
  }

  setDataCenter(name, factor) {
    this.dataCenters[name] = factor;
  }

  toCassandra() {
    const cassandraObj = { 'class' : this.class };
    Object.key(this.dataCenters).forEach(n => {
      cassandraObj[n] = this.dataCenters[n];
    });

    return cassandraObj;
  }
}