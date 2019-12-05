/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.conf.propstore.dummy;

import java.util.Objects;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf.propstore.proto2.cache.PropMap;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkOpsImpl implements ZkOps {

  private static final Logger log = LoggerFactory.getLogger(ZkOpsImpl.class);

  private final ZooKeeper zoo;

  private final ZooCallMetricsImpl metrics = new ZooCallMetricsImpl();

  public ZkOpsImpl(final ZooKeeper zoo) {
    this.zoo = zoo;
  }

  @Override
  public NodeData readNodeData(final String path, final Watcher watcher) {

    try {

      Stat stat = new Stat();
      metrics.incrGetDataCount();
      byte[] data = zoo.getData(path, watcher, stat);

      return new NodeData(stat, data);

    } catch (KeeperException.NoNodeException ex) {
      return NodeData.empty();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      return NodeData.empty();
    } catch (KeeperException ex) {
      metrics.incrErrorCount();
      throw new IllegalStateException("Could not read path '" + path + "' from zookeeper", ex);
    }
  }

  @Override
  public void setNodeData(String path, byte[] data) throws AccumuloException {
    try {
      zoo.setData(path, data, -1);
    } catch (KeeperException ex) {
      throw new AccumuloException("Set node data failed", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Get the node Stat for a path and set a watcher. If the node does not exist, the node will be
   * created - the watcher will fire a node created event and should re-add the watcher at that
   * point.
   *
   * @param path
   *          the node path
   * @param watcher
   *          a zookeeper watcher
   * @return the zookeeper node Stat
   */
  @Override
  public Stat getNodeStat(final String path, final Watcher watcher) {

    Objects.requireNonNull(path, "invalid call to get node status with a null path");

    try {
      metrics.incrExistsCount();
      Stat stat = zoo.exists(path, watcher);
      if (stat == null) {
        metrics.incrCreateCount();

        String actual = zoo.create(path, new byte[0], ZkOps.defaultAcl, ZkOps.defaultMode);
        if (path.compareTo(actual) != 0) {
          throw new IllegalStateException(
              "Invalid node created, expected '" + path + "' actual '" + actual + "'");
        }

        metrics.incrExistsCount();
        stat = zoo.exists(path, false);
        return stat;
      }

      log.trace("watching node: {} with {},", path, watcher);
      return stat;

    } catch (KeeperException ex) {
      throw new IllegalStateException(ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    return null;
  }

  @Override
  public Stat writeProps(final String rootPath, final TableId tableId, final PropMap props)
      throws AccumuloException {

    String path = ZkOps.nodePathForTableId(rootPath, tableId);

    try {

      if (props.getStat().getPzxid() == 0) {
        return createPropertyNode(path, props);
      }

      int version = props.getStat().getVersion();
      metrics.incrSetDataCount();

      Stat stat = zoo.setData(path, props.toJson(), version);
      props.setDataVersion(stat);

      return stat;

    } catch (KeeperException.BadVersionException ex) {
      metrics.incrErrorCount();
      throw new AccumuloException("Attempted concurrent update(s) to properties for table id: "
          + tableId.toString() + " Update rejected.");
    } catch (KeeperException ex) {
      metrics.incrErrorCount();
      throw new AccumuloException(
          "ZooKeeper processing exception updating properties for table id " + tableId.toString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    return null;
  }

  private Stat createPropertyNode(String path, PropMap props)
      throws KeeperException, InterruptedException {

    try {
      metrics.incrCreateCount();
      zoo.create(path, props.toJson(), ZkOps.defaultAcl, ZkOps.defaultMode);
    } catch (KeeperException.NodeExistsException ex) {
      // ignore - node already exists
    }

    metrics.incrExistsCount();
    Stat stat = zoo.exists(path, false);
    props.setDataVersion(stat);

    return stat;
  }

  @Override
  public ZooCallMetricsImpl getMetrics() {
    return metrics;
  }
}
