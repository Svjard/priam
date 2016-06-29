// Libraries
import _ from 'lodash';

/**
 * The supported replication strategories for Cassandra.
 * @const
 */
export const STRATEGIES = {
  SimpleStrategy: 'org.apache.cassandra.locator.SimpleStrategy',
  NetworkTopologyStrategy: 'org.apache.cassandra.locator.NetworkTopologyStrategy'
};
