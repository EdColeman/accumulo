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
package org.apache.accumulo.server.conf2.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.PropCache;
import org.apache.accumulo.server.conf2.PropCacheException;
import org.apache.accumulo.server.conf2.PropChangeListener;
import org.apache.accumulo.server.conf2.PropStore;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class PropStoreImpl implements PropStore {

  private static final Logger log = LoggerFactory.getLogger(PropStoreImpl.class);

  private final String configRoot;

  private final ZooKeeper zooKeeper;
  private final ZkNotificationManager zkWatchMgr;

  private final Set<PropChangeListener> listeners = ConcurrentHashMap.newKeySet();

  private final ExecutorService executorService =
      ThreadPools.createFixedThreadPool(1, "prop_change", false);

  private final PropCache cache;

  private PropStoreImpl(final ZooKeeper zooKeeper, final String instanceId, final PropCache cache) {

    this.zooKeeper = zooKeeper;
    this.cache = cache;

    configRoot = String.format("/accumulo/%s%s", instanceId, Constants.ZENCODED_CONFIG_ROOT);
    log.debug("zooKeeper configuration root node: {}", configRoot);

    zkWatchMgr = new ZkNotificationManager(configRoot, zooKeeper, this);

  }

  @VisibleForTesting
  public ZkNotificationManager getZkWatchMgr() {
    return zkWatchMgr;
  }

  @Override
  public Optional<PropEncoding> get(CacheId id) {
    log.info("CONFIG2: get - read props from store: {}", id);

    var props = cache.getProperties(id);

    if (props.isEmpty()) {
      throw new UnsupportedOperationException("YEA - we returned null");
    }

    log.info("CONFIG2: get - cache returned: id: {}, version: {}: {}", id,
        props.get().getDataVersion(), props);

    return props;
  }

  /**
   * Add or update the properties from the provided map. If validate is true, then the property name
   * / values must pass a validation check.
   *
   * @param id
   *          the cache id.
   * @param props
   *          a map of key, value pairs
   *
   * @return true if all properties set.
   * @throws PropCacheException
   *           if validate = true and invalid property passed in prop map.
   */

  @Override
  public boolean add(CacheId id, Map<String,String> props) throws PropCacheException {

    log.info("CONFIG2: set properties: {} - {}", id, props);

    var current = get(id).orElseThrow(() -> new IllegalStateException(
        "Tried to add properties for " + id + ", create should be called first."));

    for (Map.Entry<String,String> e : props.entrySet()) {

      current.addProperty(e.getKey(), e.getValue());
    }

    writeToStore(id, current);
    // TODO exception handling? Always returning true - so not very helpful

    return true;
  }

  @Override
  public boolean create(CacheId id, Map<String,String> props) throws PropCacheException {
    try {
      var propPath = getPropPath(id);

      if (Objects.nonNull(zooKeeper.exists(propPath, false))) {
        return add(id, props);
      }

      writeInitProps(id, props);
    } catch (KeeperException ex) {
      log.warn("Could not create initial props", ex);
      return false;
    } catch (InterruptedException ex) {
      log.warn("Could not create initial props", ex);
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted checking zookeeper node", ex);
    }

    return add(id, props);
  }

  @Override
  public boolean removeProperties(CacheId id, Collection<String> keys) {
    var current = get(id).orElse(new PropEncodingV1());
    keys.forEach(current::removeProperty);
    writeToStore(id, current);
    return true;
  }

  public boolean add(final CacheId id, final String name, final String value) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public void clear(CacheId id) {
    cache.clear(id);
  }

  @Override
  public void deleteProperties(CacheId id) {
    deleteFromStore(id);
  }

  @Override
  public void clearAll() {
    cache.clearAll();
  }

  @Override
  public void register(PropChangeListener listener) {
    if (Objects.nonNull(listener)) {
      listeners.add(listener);
    }
  }

  @Override
  public void deregister(PropChangeListener listener) {
    if (Objects.nonNull(listener)) {
      listeners.remove(listener);
    }
  }

  @Override
  public void changeEvent(final CacheId id) {
    for (PropChangeListener watcher : listeners) {
      executorService.submit(() -> watcher.changeEvent(id));
    }
  }

  @Override
  public void deleteEvent(final CacheId id) {
    for (PropChangeListener watcher : listeners) {
      executorService.submit(() -> watcher.deleteEvent(id));
    }
  }

  @Override
  public boolean isReady() {
    return zkWatchMgr.isReady();
  }

  private String getPropPath(CacheId id) {
    return String.format("%s/%s", configRoot, id.nodeName());
  }

  public void deleteFromStore(CacheId id) {

    var propPath = getPropPath(id);
    var current = get(id);
    if (current.isEmpty()) {
      return;
    }

    log.debug("PropStore: deleteFromStore - Removing props at {}", propPath);
    try {
      zooKeeper.delete(propPath, current.get().getDataVersion());
      log.debug("PropStore: deleteFromStore - completed {}", propPath);
    } catch (KeeperException.NoNodeException ex) {
      log.debug("Did not delete id {}, path {}, node does not exist", id, propPath);
    } catch (KeeperException ex) {
      throw new IllegalStateException("Failed to remove path from zookeeper" + propPath, ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted removing path from zookeeper " + propPath, ex);
    }
  }

  @Override
  public PropEncoding readFromStore(CacheId id) {

    var propPath = getPropPath(id);

    log.trace("PropStore: readFromStore - Checking for props at {}", propPath);

    PropEncoding props;

    try {
      var delays = new long[] {1_000, 3_000, 5_000, 10_000, 10_000};

      int count = 0;
      while (Objects.isNull(zooKeeper.exists(propPath, false)) && count < delays.length) {
        try {
          log.warn("Waiting " + delays[count] + " ms for zookeeper props: " + id);
          Thread.sleep(delays[count++]);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(
              "Interrupted waiting for props in zookeeper for id " + id);
        }
      }

      Stat stat = zooKeeper.exists(propPath, false);
      if (Objects.isNull(stat)) {
        var c = zooKeeper.getChildren(configRoot, false);
        for (String s : c) {
          log.warn("Looking for props - current children: {}", c);
        }

        // TODO - if returning default - what about watcher?
        // no config node - create node with empty props
        log.info("CONFIG2: No node at {}, returning empty props", propPath);
        // props = writeInitProps(id);
        throw new IllegalStateException("No prop node for " + id);
      } else {

        // read
        byte[] r = zooKeeper.getData(propPath, zkWatchMgr, null);

        props = new PropEncodingV1(r);
        log.trace("Props for {}, returning {}", propPath, props.print(true));
      }

      return props;

    } catch (KeeperException ex) {
      throw new IllegalStateException("Could not get properties for " + propPath, ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted getting properties for " + propPath, ex);
    }
  }

  private PropEncoding writeInitProps(final CacheId id, Map<String,String> initProps)
      throws KeeperException, InterruptedException {

    log.debug("CONFIG2: Write initial props for {}", id);

    PropEncodingV1 props = new PropEncodingV1();
    props.addProperties(initProps);

    writeToStore(id, props);
    return readFromStore(id);
  }

  /**
   * Write encoded props to zookeeper - if the znode does not exist, it is created. If the znode
   * does exist it will attempt to update. If the data version of the encoded props does not match
   * the expected version in zookeeper the write is rejected with an exception - it is assumed that
   * another process has modified the data - the caller needs to decide what to do. Options are pass
   * the exception the call stack - or if there is a merge strategy, re-read and retry.
   *
   * @param id
   *          the cache id
   * @param props
   *          the encoded properties.
   */
  @Override
  public void writeToStore(CacheId id, PropEncoding props) {
    var propPath = getPropPath(id);
    log.info("CONFIG2: Setting props at {}, version: {}, values: {}", propPath,
        props.getDataVersion(), props.getAllProperties());
    try {
      Stat stat = zooKeeper.exists(propPath, false);
      log.debug("CONFIG2: writeToStore - path: {} for id: {} current zk stat: {}", propPath, id,
          ZooUtil.printStat(stat));
      if (Objects.isNull(stat)) {
        try {
          var name =
              zooKeeper.create(propPath, props.toBytes(), ZooUtil.PUBLIC, CreateMode.PERSISTENT);
          log.trace("CONFIG2: Created zooKeeper node: {}", name);
          return;
        } catch (KeeperException.NodeExistsException ex) {
          log.debug(
              "Path: {} for id: {} was created after checked for exists - will continue and try to update",
              propPath, id);
          // for debug print
          stat = zooKeeper.exists(propPath, false);
        }
      }

      // Stat x = zooKeeper.exists(propPath, false);

      var zkExpectedVersion = props.getDataVersion();

      log.debug("would like to update: {}", ZooUtil.printStat(stat));
      log.debug("update is version: {} - {} ", zkExpectedVersion, props.print(true));

      zooKeeper.setData(propPath, props.toBytes(), zkExpectedVersion);

      cache.clear(id);

    } catch (KeeperException.BadVersionException ex) {
      PropEncoding inStore = readFromStore(id);
      log.warn("CONFIG2 - BAD_VERSION: Current Zookeeper values {}", inStore.getAllProperties());
      log.warn("CONFIG2 - BAD_VERSION: expected {}", props.print(true));
      throw new IllegalStateException(
          "Could not set properties for " + propPath + " unexpected zookeeper version", ex);
    } catch (KeeperException ex) {
      throw new IllegalStateException("Could not set properties for " + propPath, ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted setting properties for " + propPath, ex);
    }
  }

  /**
   * Called when cacheId is removed by the cache - remove any watchers.
   *
   * @param id
   *          the cache id
   */
  @Override
  public void cleanUp(CacheId id) {
    log.warn("CONFIG2 - Cleanup called for id: {}", id);
  }

  public static class Builder {

    private ZooKeeper zooKeeper;
    private PropCache cache;
    private String instanceId;

    public PropStoreImpl build() {

      Objects.requireNonNull(zooKeeper, "Valid ZooKeeper instance must be supplied");
      Objects.requireNonNull(cache, "A valid PropCache implementation must be provided");
      Objects.requireNonNull(instanceId, "Valid instance ID must be supplied");

      return new PropStoreImpl(zooKeeper, instanceId, cache);

    }

    public Builder withZk(final ZooKeeper zooKeeper) {
      this.zooKeeper = zooKeeper;
      return this;
    }

    public Builder forInstance(String instanceID) {
      this.instanceId = instanceID;
      return this;
    }

    public Builder withCache(final PropCache cache) {
      this.cache = cache;
      return this;
    }
  }
}
