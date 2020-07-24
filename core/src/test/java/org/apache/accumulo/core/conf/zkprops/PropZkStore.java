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
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.conf.zkprops.serdes.PropMapSerdes;
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

  private static final int ZK_WRITE_RETRY_LIMIT = 3;

  private final ZooKeeper zkClient;
  private final Map<ZkPropPath,CacheablePropMap> cache = new HashMap<>();

  private final PropMapSerdes serdes = new PropMapSerdes();

  // METRICS Placeholder
  // AtomicInteger zkWriteErrorCount = new AtomicInteger(0);
  // AtomicInteger cacheHitsCount = new AtomicInteger(0);
  AtomicInteger cacheInvalidVerCount = new AtomicInteger(0);

  public int getCacheInvalidVerCount() {
    return cacheInvalidVerCount.get();
  }


  public PropZkStore(final ZooKeeper zkClient) {
    this.zkClient = zkClient;
  }

  @Override
  public CacheablePropMap get(ZkPropPath path) {
    return cache.get(path);
  }

  @Override
  public void store(CacheablePropMap node) {}

  @Override
  public void deleteProp(ZkPropPath path, String propName){
    throw new UnsupportedOperationException("todo");
  }

  @Override
  public void deleteAll(ZkPropPath path){
    throw new UnsupportedOperationException("todo");
  }

  @Override
  public void cloneProperties(final ZkPropPath source, final ZkPropPath dest){
    throw new UnsupportedOperationException("todo");
  }

  @Override
  public void setProperties(Collection<Map.Entry<String, String>> properties){
    throw new UnsupportedOperationException("todo");
  }

  @Override
  public void setProperty(ZkPropPath path, String propName, String value) {

    var retryCount = 0;

    try {

      while (retryCount++ < ZK_WRITE_RETRY_LIMIT) {

        // if entry in local cache, use entry id
        CacheablePropMap entry = cache.computeIfAbsent(path, n -> getFromZookeeper(path, true));

        if (Objects.isNull(entry)) {
          continue;
        }

        log.debug("Lookup returned {}", entry);

        entry.setProperty(propName, value);

        if (save(entry)) {
          break;
        } else {
          // save failed, remove from cache because cache version is out of date. retry if
          // more attempts are possible.
          cache.remove(path);
          cacheInvalidVerCount.incrementAndGet();
          try {
            Thread.sleep(100 * retryCount);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        }
      }
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
   * @param create
   *          if true - create the node if missing, otherwise return null.
   * @return the properties stored in zookeeper.
   */
  private CacheablePropMap getFromZookeeper(final ZkPropPath path, final boolean create) {

    Stat stat = new Stat();

    try {

      byte[] data = zkClient.getData(path.canonical(), false, stat);
      PropMap propMap = serdes.fromBytes(data);
      return new CacheablePropMap(path, stat.getVersion(), propMap);

    } catch (KeeperException.NoNodeException ex) {
      if (create) {
        return createNode(path);
      }
    } catch (KeeperException | IOException ex) {
      log.info("general zookeeper exception looking update path: {}", path, ex);
      throw new IllegalStateException("Error accessing zookeeper path: " + path, ex);
    } catch (InterruptedException ex) {
      // propagate the interrupt
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted accessing zookeeper for path " + path, ex);
    }

    log.debug("Don't know what to do?");
    return null;

  }

  private CacheablePropMap createNode(final ZkPropPath path) {

    try {
      Stat stat = new Stat();
      log.debug("Creating node");
      PropMap propMap = new PropMap(path);
      zkClient.create(path.canonical(), serdes.toBytes(propMap, PropMap.class),
          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, stat);
      return new CacheablePropMap(path, stat.getVersion(), propMap);
    } catch (InterruptedException ex) {
      // propagate the interrupt
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted accessing zookeeper for path " + path, ex);
    } catch (KeeperException | IOException ex) {
      log.info("general zookeeper exception looking update path: {}", path, ex);
    }
    return null;
  }

  /**
   * Write entry updates back to zookeeper. Use an optimistic write that uses the version in cache
   * entry as the expected version that is in zookeeper - if the version has changed, update from
   * zookeeper and try again
   *
   * @param entry
   *          a cache entry with a valid version
   * @return true is zookeeper write succeeded, false if the data version do not match
   */
  private boolean save(CacheablePropMap entry) {

    try {

      Stat stat = zkClient.setData(entry.getPath().canonical(),
          serdes.toBytes(entry.getPropMap(), PropMap.class), entry.getVersion());

      entry.updateVersion(stat.getVersion());

      log.debug("Save? Save what? {}, {}", entry, ZooKeeperTestingServer.prettyStat(stat));

      return true;

    } catch (KeeperException.BadVersionException ex) {
      return false;
    } catch (Exception ex) {
      throw new IllegalStateException("Could not save data to zookeeper", ex);
    }
  }

}
