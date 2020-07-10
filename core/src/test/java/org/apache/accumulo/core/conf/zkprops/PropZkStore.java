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
package org.apache.accumulo.core.conf.zkprops;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a PropStore that uses ZooKeeper.
 */
public class PropZkStore implements PropStore {

  private static final Logger log = LoggerFactory.getLogger(PropZkStore.class);

  private final ZooKeeper zkClient;

  PropMapSerdes serdes = new PropMapSerdes();

  private Map<String,CacheablePropMap> cache = new HashMap<>();

  // fake transaction id;
  private int txid = 1;

  public PropZkStore(final ZooKeeper zkClient) {
    this.zkClient = zkClient;
  }

  @Override
  public CacheablePropMap get(String path) {
    return cache.get(path);
  }

  @Override
  public void store(CacheablePropMap node) {}

  @Override
  public void setProperty(PropId.Scope scope, String path, String propName, String value) {

    try {

      // if entry in local cache, use entry id
      CacheablePropMap entry = cache.computeIfAbsent(path, n -> lookup(path, true));

      log.debug("Lookup returned {}", entry);

      entry.setProperty(propName, value);

      save(entry);

    } catch (IllegalStateException ex) {
      throw new IllegalStateException("Received interrupt trying to set path " + path, ex);
    }

    // use node id
    // node.setProp(propName, value);

    // catch and handle KeeperException.BadVersion

    // ? if same, update anyway?

  }

  /**
   * Stub zookeeper functionality. With Zookeeper this would check if the node exists and either
   * retrieve it or create a new node.
   *
   * @param path
   *          path in zookeeper
   * @return the properties stored in zookeeper.
   * @Param create if true - create the node if missing, otherwise return null.
   */
  private CacheablePropMap lookup(final String path, final boolean create) {

    try {

      Stat stat = zkClient.exists(path, false);

      log.debug("initial lookup {}", ZooKeeperTestingServer.prettyStat(stat));

      if (Objects.nonNull(stat)) {
        byte[] data = zkClient.getData(path, false, stat);
        PropMap propMap = serdes.fromBytes(data);
        CacheablePropMap entry = new CacheablePropMap(path, stat.getVersion(), propMap);

        if (stat.getVersion() == entry.getVersion()) {
          log.debug("Data in zookeeper matches current version");
        } else {
          log.debug("Updating version to match zookeeper");
          entry.updateVersion(stat.getVersion());
        }

        return entry;

      }

      if (create) {
        log.debug("Creating node");
        stat = new Stat();
        PropMap propMap = new PropMap(path);
        zkClient.create(path, serdes.compress(propMap, PropMap.class), ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT, stat);
        return new CacheablePropMap(path, stat.getVersion(), propMap);
      }

      log.debug("Don't know what to do?");
      return null;

    } catch (KeeperException ex) {
      throw new IllegalStateException("Could not lookup node for path " + path, ex);
    } catch (InterruptedException ex) {
      // propagate the interrupt
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted accessing zookeeper for path " + path, ex);
    } catch (IOException ex) {
      throw new IllegalStateException("Could not decode prop data", ex);
    }
  }

  /**
   * Write entry updates back to zookeeper. Use version in cache entry as expected value in the put.
   *
   * @param entry
   *          a cache entry with a valid version
   */
  private void save(CacheablePropMap entry) {
    try {

      Stat stat = zkClient.setData(entry.getPath(),
          serdes.compress(entry.getPropMap(), PropMap.class), entry.getVersion());
      log.debug("Save? Save what? {}", entry, ZooKeeperTestingServer.prettyStat(stat));

      // node.updateVersion(node.getVersion() + 1);
    } catch (Exception ex) {
      // TODO replace with actual handling.
      ex.printStackTrace();
    }
  }

  private int getTxId() {
    return ++txid;
  }
}
