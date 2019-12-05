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
package org.apache.accumulo.server.conf.propstore;

import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class ZkBackingStoreMgr implements BackingStore {

  private final BackingStoreMetrics metrics;
  // private final ZkPropCacheImpl zkPropStore;

  private final ZooKeeper zooKeeper;
  private final String accumuloInstanceId;

  private static ZkBackingStoreMgr _instance;

  private ZkBackingStoreMgr(final String accumuloInstanceId, final ZooKeeper zooKeeper) {

    this.accumuloInstanceId = accumuloInstanceId;
    this.zooKeeper = zooKeeper;

    metrics = new BackingStoreMetrics(this);

    ZkBackingStoreMgr._instance = this;

    // this.zkPropStore = ZkPropCacheImpl.getInstance();
    // zkPropStore.registerStore(this);
  }

  public AccumuloConfiguration getTableConfig(final TableId tableId) {
    return null;
  }

  public void createTableCache(final NamespaceId namespaceId, final TableId tableId) {

  }

  @Override
  public Map<Property,String> load(TableId tableId) {
    metrics.loadCall();
    long start = System.nanoTime();
    try {

      // lookup.

      // not found, either not created or has been deleted, what now?
      // set watcher to see if table props get created.
      // what about deletions?

      // zkPropStore.setTableProps(tableId, Collections.emptyMap());

    } finally {

      metrics.recordZkLookupTime(System.nanoTime() - start);
    }
    return Collections.emptyMap();
  }

  public static class Factory {

    private String accumuloInstanceId;
    private ZooKeeper zoo;

    public Factory() {}

    public Factory forAccumuloInstance(final String accumuloInstanceId) {
      this.accumuloInstanceId = accumuloInstanceId;
      return this;
    }

    public Factory withZooKeeper(final ZooKeeper zoo) {
      this.zoo = zoo;
      return this;
    }

    public synchronized ZkBackingStoreMgr build() {

      if (ZkBackingStoreMgr._instance != null) {
        return ZkBackingStoreMgr._instance;
      }

      if (accumuloInstanceId == null) {
        throw new IllegalArgumentException("Accumulo instance Id must be provided.");
      }

      if (zoo == null) {
        throw new IllegalArgumentException("ZooKeeper client must be provided.");
      }

      try {

        // validate instance exists in zookeeper.
        var instancePath = Constants.ZROOT + "/" + accumuloInstanceId;
        if (zoo.exists(instancePath, false) == null) {
          throw new IllegalArgumentException("Invalid instance is, the instance '" + instancePath
              + "' does not exist in zookeeper.  Will not continue.");
        }

        // validate the config path for the instance is in zookeeper.
        var zConfigRoot = instancePath + Constants.ZCONFIG;
        if (zoo.exists(zConfigRoot, false) == null) {
          throw new IllegalArgumentException(
              "The path '" + zConfigRoot + "' does not exist in zookeeper, will not continue.");
        }

      } catch (KeeperException ex) {
        throw new IllegalStateException("Could not read instance configuration node from zookeeper",
            ex);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }

      return new ZkBackingStoreMgr(accumuloInstanceId, zoo);
    }
  }
}
