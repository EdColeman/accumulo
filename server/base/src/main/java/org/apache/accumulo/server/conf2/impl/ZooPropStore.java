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

import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.server.conf2.PropStoreException.REASON_CODE.INTERRUPT;
import static org.apache.accumulo.server.conf2.PropStoreException.REASON_CODE.ZK_ERROR;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.conf2.PropCache;
import org.apache.accumulo.server.conf2.PropCacheId;
import org.apache.accumulo.server.conf2.PropChangeListener;
import org.apache.accumulo.server.conf2.PropStore;
import org.apache.accumulo.server.conf2.PropStoreException;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooPropStore implements PropStore {

  private static final Logger log = LoggerFactory.getLogger(ZooPropStore.class);

  private final ZooKeeper zooKeeper;
  private final String instanceId;
  private final ZkChangeWatcher watcher;

  private final ReadyMonitor zkReadyMon = new ReadyMonitor("props-zk-session");

  private final PropCache propCache;
  private Map<String,String> fixedProps = null;

  public ZooPropStore(final String instanceId, final ZooKeeper zooKeeper,
      final PropCache propCache) {

    this.instanceId = requireNonNull(instanceId, "instanceId cannot be null");
    this.zooKeeper = requireNonNull(zooKeeper, "zooKeeper cannot be null");
    this.propCache = propCache;

    watcher = new ZkChangeWatcher(propCache, zooKeeper, zkReadyMon);

    try {
      var path = PropCacheId.getConfigRoot(instanceId);
      Stat s = zooKeeper.exists(path, watcher);
      if (Objects.nonNull(s)) {
        log.info("ZooKeeper connection exists");
        zkReadyMon.setReady();
      } else {
        log.warn("Expected system configuration node {} does not exist", path);
      }

    } catch (KeeperException | InterruptedException ex) {
      // TODO - handle exception
      log.warn("Initialization error", ex);
    }

  }

  /**
   * Create initial blank system props for the instance. If the node already exists, no action is
   * performed.
   *
   * @param instanceId
   *          the instance id.
   * @param zooKeeper
   *          a zooKeeper client
   * @return true if empty props create, false if the node exists.
   */
  public synchronized static boolean init(final String instanceId, final ZooKeeper zooKeeper) {
    PropCacheId sysPropsId = PropCacheId.forSystem(instanceId);
    return initNode(instanceId, zooKeeper, sysPropsId, null);
  }

  /**
   * Create initial property node. If the node already exists, no action is performed.
   *
   * @param instanceId
   *          the instance id.
   * @param zooKeeper
   *          a zooKeeper client
   * @param propCacheId
   *          the propCacheId that will be initialized / created.
   * @param initProps
   *          property key, value string pairs - if null, an empty node is created.
   * @return true if empty props create, false if the node exists.
   */
  public synchronized static boolean initNode(final String instanceId, final ZooKeeper zooKeeper,
      final PropCacheId propCacheId, final Map<String,String> initProps) {
    PropEncoding props = new PropEncodingV1();
    try {

      log.trace("Property init for path: {}", propCacheId.path());

      if (Objects.nonNull(zooKeeper.exists(propCacheId.path(), false))) {
        log.debug("Node {} already exists in zooKeeper - skipping initial prop write", propCacheId);
        return false;
      }
      props.addProperties(initProps);

      zooKeeper.create(propCacheId.path(), props.toBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
      return true;

    } catch (KeeperException.NodeExistsException ex) {
      // the node was created by something else.
      return false;
    } catch (KeeperException ex) {
      throw new IllegalStateException("Failed to create system props initial node", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted initializing prop store init path", ex);
    }
  }

  private void waitForConnection() throws PropStoreException {

    if (zkReadyMon.test()) {
      return;
    }

    // not ready block or throw error if it times out.
    try {
      int zooSessionTimeout = zooKeeper.getSessionTimeout();
      zkReadyMon.isReady(zooSessionTimeout);
    } catch (IllegalStateException ex) {
      if (Thread.interrupted()) {
        throw new PropStoreException(INTERRUPT, "Interrupted waiting fro ZooKeeper connection",
            null);
      }
      throw new PropStoreException(PropStoreException.REASON_CODE.OTHER,
          "Failed to get zooKeeper connection", ex);
    }

    propCache.removeAll();

  }

  // TODO - evaluate returning the props instead of boolean.
  @Override
  public boolean create(PropCacheId propCacheId, Map<String,String> props)
      throws PropStoreException {
    try {
      PropEncoding encoded = new PropEncodingV1();
      if (Objects.nonNull(props)) {
        encoded.addProperties(props);
      }

      var path = propCacheId.path();

      zooKeeper.create(path, encoded.toBytes(), ZooUtil.PUBLIC, CreateMode.PERSISTENT);

      Stat stat = zooKeeper.exists(path, watcher);

      propCache.put(propCacheId, encoded);

    } catch (KeeperException | InterruptedException ex) {
      log.info("Create failed", ex);
      throw new PropStoreException(ZK_ERROR,
          "Failed to create properties for propCacheId " + propCacheId, ex);
    }
    return false;
  }

  @Override
  public PropEncoding get(final PropCacheId propCacheId) throws PropStoreException {
    Objects.requireNonNull(propCacheId, "prop store get() - Must provide propCacheId");

    try {

      waitForConnection();

      var cached = propCache.get(propCacheId);
      if (cached.isPresent()) {
        return cached.get();
      }

      Stat stat = new Stat();
      byte[] data = zooKeeper.getData(propCacheId.path(), watcher, stat);
      PropEncoding props = new PropEncodingV1(data);
      propCache.put(propCacheId, props);
      return props;

    } catch (KeeperException ex) {
      throw new PropStoreException("Failed to read properties for propCacheId " + propCacheId, ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new PropStoreException(ZK_ERROR,
          "Failed to read properties for propCacheId " + propCacheId, ex);
    }
  }

  private void writeAndCache(final PropCacheId propCacheId, final PropEncoding props)
      throws InterruptedException, KeeperException {

    log.info("Writing: {}, {}", propCacheId, props.print(true));
    zooKeeper.setData(propCacheId.path(), props.toBytes(), props.getExpectedVersion());
    propCache.put(propCacheId, props);
  }

  @Override
  public boolean update(final PropCacheId propCacheId, final Map<String,String> properties)
      throws PropStoreException {
    Objects.requireNonNull(propCacheId, "prop store update() - Must provide propCacheId");
    try {
      var path = propCacheId.path();

      var cached = propCache.get(propCacheId);
      if (cached.isPresent()) {
        PropEncoding cachedProps = cached.get();
        cachedProps.addProperties(properties);
        try {
          writeAndCache(propCacheId, cachedProps);
          log.info("Updated cached - wrote: {}", cachedProps.print(true));
          return true;
        } catch (KeeperException.BadVersionException ex) {
          log.info("Unexpected version - cache and zookeeper versions differ");
          Stat s = zooKeeper.exists(path, false);
          log.info("Zk contains version: {}", s.getVersion());
          // cached version differs - will read from zk and retry
        }
      }

      byte[] data = zooKeeper.getData(path, false, null);
      PropEncoding current = new PropEncodingV1(data);
      log.info("Updated cached - read: {}", current.print(true));
      current.addProperties(properties);
      writeAndCache(propCacheId, current);
      log.info("Updated cached - wrote: {}", current.print(true));

    } catch (KeeperException | InterruptedException ex) {
      throw new PropStoreException(ZK_ERROR,
          "Failed to update properties for propCacheId " + propCacheId, ex);
    }
    return true;
  }

  @Override
  public void delete(final PropCacheId propCacheId) throws PropStoreException {
    Objects.requireNonNull(propCacheId, "prop store delete() - Must provide propCacheId");
    try {
      final String path = propCacheId.path();
      Stat stat = new Stat();
      zooKeeper.delete(path, stat.getVersion());
      propCache.remove(propCacheId);
    } catch (KeeperException | InterruptedException ex) {
      throw new PropStoreException(ZK_ERROR,
          "Failed to update properties for propCacheId " + propCacheId, ex);
    }
  }

  @Override
  public boolean removeProperties(final PropCacheId propCacheId, final Collection<String> keys)
      throws PropStoreException {
    var path = propCacheId.path();
    Stat stat = new Stat();
    try {
      byte[] data = zooKeeper.getData(path, false, stat);
      PropEncoding current = new PropEncodingV1(data);
      keys.forEach(current::removeProperty);
      zooKeeper.setData(path, current.toBytes(), stat.getVersion());
    } catch (KeeperException | InterruptedException ex) {
      throw new PropStoreException(ZK_ERROR,
          "Failed to update properties for propCacheId " + propCacheId, ex);
    }
    return false;
  }

  @Override
  public synchronized Map<String,String> readFixed() {

    if (Objects.nonNull(fixedProps)) {
      return fixedProps;
    }

    fixedProps = new HashMap<>();

    PropCacheId systemId = PropCacheId.forSystem(instanceId);
    try {

      Map<String,String> propsRead;

      byte[] data = zooKeeper.getData(systemId.path(), false, null);
      // TODO - this might be be required - after init system props should always exist
      if (data != null) {
        propsRead = new PropEncodingV1(data).getAllProperties();
      } else {
        propsRead = new HashMap<>();
      }

      for (Property p : Property.fixedProperties) {
        fixedProps.put(p.getKey(), propsRead.getOrDefault(p.getKey(), p.getDefaultValue()));
      }

      return fixedProps;

    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to read system properties", ex);
    }
  }

  @Override
  public void registerAsListener(PropCacheId propCacheId, PropChangeListener listener) {
    watcher.registerListener(propCacheId, listener);
  }

  private static class ZkChangeWatcher implements Watcher {

    private final ZooKeeper zooKeeper;
    private final PropCache cache;

    private final ExecutorService executorService =
        ThreadPools.createFixedThreadPool(1, "zoo_change_update", false);

    private final Map<PropCacheId,Set<PropChangeListener>> listeners = new HashMap<>();
    private final ReentrantReadWriteLock.ReadLock listenerReadLock;
    private final ReentrantReadWriteLock.WriteLock listenerWriteLock;
    private final ReadyMonitor zkReadyMonitor;

    public ZkChangeWatcher(final PropCache cache, final ZooKeeper zooKeeper,
        final ReadyMonitor zkReadyMonitor) {
      this.cache = cache;
      this.zooKeeper = zooKeeper;
      this.zkReadyMonitor = zkReadyMonitor;

      ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
      listenerReadLock = rwLock.readLock();
      listenerWriteLock = rwLock.writeLock();
    }

    public void registerListener(final PropCacheId propCacheId, final PropChangeListener listener) {
      listenerWriteLock.lock();
      try {
        Set<PropChangeListener> set = listeners.computeIfAbsent(propCacheId, s -> new HashSet<>());
        set.add(listener);
      } finally {
        listenerWriteLock.unlock();
      }
    }

    /**
     * Process the ZooKeeper event. This method does not reset the watcher. Subscribers are notified
     * of the change - if they call get to update and respond to the change the watcher will be
     * (re)set then. This help clean up watcher by not automatically re-adding the wather on the
     * event but only if being used.
     *
     * @param event
     *          ZooKeeper event.
     */
    @Override
    public void process(final WatchedEvent event) {

      log.info("ZK event: {} - {}", event, event.getPath());

      String path;
      switch (event.getType()) {
        case NodeDataChanged:
          path = event.getPath();

          log.info("handle change event");

          PropCacheId.fromPath(path).ifPresent(cacheId -> {

            cache.remove(cacheId);

            Set<PropChangeListener> snapshot = getListenerSnapshot(cacheId);

            if (Objects.nonNull(snapshot)) {
              executorService
                  .submit(new ZkWatchEventProcessor.ZkChangeEventProcessor(cacheId, snapshot));
            }
          });

          break;
        case NodeDeleted:
          path = event.getPath();

          PropCacheId.fromPath(path).ifPresent(cacheId -> {

            cache.remove(cacheId);

            Set<PropChangeListener> snapshot = getListenerSnapshot(cacheId);

            if (Objects.nonNull(snapshot)) {
              executorService
                  .submit(new ZkWatchEventProcessor.ZkDeleteEventProcessor(cacheId, snapshot));
            }
          });

          break;
        case None:
          Event.KeeperState state = event.getState();
          switch (state) {
            // pause - could reconnect
            case ConnectedReadOnly:
            case Disconnected:
              log.info("disconnected");
              zkReadyMonitor.clearReady();
              break;

            // okay
            case SyncConnected:
              log.info("Connected");
              zkReadyMonitor.setReady();
              break;

            // terminal - never coming back.
            case Expired:
            case Closed:
              log.info("connection closed");
              zkReadyMonitor.clearReady();
              break;

            default:
              log.trace("ignoring zooKeeper state: {}", state);
          }
          break;
        default:
          break;
      }

    }

    private Set<PropChangeListener> getListenerSnapshot(final PropCacheId propCacheId) {

      Set<PropChangeListener> snapshot = null;
      listenerReadLock.lock();
      try {
        Set<PropChangeListener> set = listeners.get(propCacheId);
        if (Objects.nonNull(set)) {
          snapshot = Set.copyOf(set);
        }

      } finally {
        listenerReadLock.unlock();
      }
      return snapshot;
    }

    private void rewatch(String path) {
      try {
        zooKeeper.exists(path, this);
      } catch (KeeperException | InterruptedException ex) {
        throw new IllegalStateException(ex);
      }
    }

  }
}
