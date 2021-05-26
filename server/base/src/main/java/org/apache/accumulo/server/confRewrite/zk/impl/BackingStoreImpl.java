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
package org.apache.accumulo.server.confRewrite.zk.impl;

import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.confRewrite.zk.BackingStore;
import org.apache.accumulo.server.confRewrite.zk.DataChangeEventHandler;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class BackingStoreImpl implements BackingStore {

  private final String instanceId;
  private final ZooKeeper zooKeeper;
  private final DataChangeEventHandler dataChangeEventHandler;
  private final ZkEventProcessor eventProcessor;

  public BackingStoreImpl(final String instanceId, final ZooKeeper zooKeeper,
      final DataChangeEventHandler dataChangeEventHandler) {
    this(instanceId, zooKeeper, dataChangeEventHandler,
        new ZkEventProcessor(dataChangeEventHandler));
  }

  public BackingStoreImpl(final String instanceId, final ZooKeeper zooKeeper,
      final DataChangeEventHandler dataChangeEventHandler, final ZkEventProcessor eventProcessor) {
    this.instanceId = instanceId;
    this.zooKeeper = zooKeeper;
    this.dataChangeEventHandler = dataChangeEventHandler;
    this.eventProcessor = eventProcessor;

    setInitialWatch();
  }

  /**
   * The initial watch sets a zookeeper watcher on the root property node to receive connection
   * events.
   */
  private void setInitialWatch() {
    final String propRootPath = CacheId.getConfigRoot(instanceId);
    try {
      zooKeeper.exists(propRootPath, eventProcessor);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "Failed to read property root path " + propRootPath + " in ZooKeeper");
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted reading property root path " + propRootPath + " from ZooKeeper", ex);
    }
  }

  @Override
  public boolean createInStore(CacheId id, PropEncoding props) {
    return false;
  }

  @Override
  public PropEncoding readFromStore(final CacheId id) {
    return readFromStore(id, new Stat());
  }

  @Override
  public PropEncoding readFromStore(final CacheId id, final Stat stat) {
    return null;
  }

  @Override
  public Stat readZkStat(final CacheId id) {
    try {
      return zooKeeper.exists(id.path(), false);
    } catch (KeeperException ex) {
      throw new IllegalStateException("Could not read node stat for " + id, ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted reading stat from zookeeper " + id, ex);
    }
  }

  @Override
  public void writeToStore(final CacheId id, final PropEncoding props) {

  }

  @Override
  public void deleteFromStore(final CacheId id) {

  }

}
