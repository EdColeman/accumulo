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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.PropCache;
import org.apache.accumulo.server.conf2.PropStore;
import org.apache.accumulo.server.conf2.PropWatcher;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Ticker;

public class ZooPropStore implements PropCache, PropStore {

  private static final Logger log = LoggerFactory.getLogger(ZooPropStore.class);

  private final String configRoot;

  private final ZooKeeper zooKeeper;
  private final ZkNotificationManager zkWatchMgr;

  // private final Notifier notifier;

  private final Set<PropWatcher> watchers = ConcurrentHashMap.newKeySet();
  private final ExecutorService executorService =
      ThreadPools.createFixedThreadPool(1, "prop_change", false);

  private final CacheWrapper cache;

  private ZooPropStore(final ZooKeeper zooKeeper, final String instanceId) {
    this(zooKeeper, instanceId, null);
  }

  private ZooPropStore(final ZooKeeper zooKeeper, final String instanceId, Ticker ticker) {

    this.zooKeeper = zooKeeper;

    configRoot = String.format("/accumulo/%s/config2", instanceId);
    log.debug("zooKeeper configuration root node: {}", configRoot);

    if (Objects.isNull(ticker)) {
      cache = new CacheWrapper(this);
    } else {
      cache = new CacheWrapper(this, ticker);
    }

    zkWatchMgr = new ZkNotificationManager(configRoot, zooKeeper, this);

  }

  // testing.
  public ZkNotificationManager getZkWatchMgr() {
    return zkWatchMgr;
  }

  @Override
  public Optional<PropEncoding> getProperties(CacheId id) {
    return cache.getProperties(id);
  }

  @Override
  public boolean setProperties(CacheId id, Map<String,String> props)
      throws IllegalArgumentException {
    var current = getProperties(id).orElse(new PropEncodingV1());

    for (Map.Entry<String,String> e : props.entrySet()) {
      if (TablePropUtil.isPropertyValid(e.getKey(), e.getValue())) {
        current.addProperty(e.getKey(), e.getValue());
      } else {
        throw new IllegalArgumentException(String
            .format("Invalid property for %s, key: %s, value: %s", id, e.getKey(), e.getValue()));
      }
    }
    writeToStore(id, current);
    return true;
  }

  @Override
  public boolean removeProperties(CacheId id, Collection<String> keys) {
    var current = getProperties(id).orElse(new PropEncodingV1());
    keys.forEach(k -> current.removeProperty(k));
    writeToStore(id, current);
    return true;
  }

  @Override
  public boolean setProperty(final CacheId id, final String name, final String value) {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public void clear(CacheId id) {
    cache.clear(id);
  }

  @Override
  public void clearAll() {
    cache.clearAll();
  }

  @Override
  public void register(PropWatcher listener) {
    if (Objects.nonNull(listener)) {
      watchers.add(listener);
    }
  }

  @Override
  public void deregister(PropWatcher listener) {
    if (Objects.nonNull(listener)) {
      watchers.remove(listener);
    }
  }

  @Override
  public void changeEvent(final CacheId id) {
    for (PropWatcher watcher : watchers) {
      executorService.submit(() -> watcher.changeEvent(id));
    }
  }

  @Override
  public boolean isReady() {
    return zkWatchMgr.isReady();
  }

  private String getPropPath(CacheId id) {
    return String.format("%s/%s", configRoot, id.nodeName());
  }

  @Override
  public PropEncoding readFromStore(CacheId id) {

    String propPath = getPropPath(id);

    log.trace("readFromStore - Checking for props at {}", propPath);

    try {
      Stat stat = zooKeeper.exists(propPath, false);
      if (Objects.isNull(stat)) {
        // TODO - if returning default - what about watcher?
        // no config node - create node with empty props
        log.trace("No node at {}, returning empty props", propPath);
        return new PropEncodingV1();
      }
      // read
      byte[] r = zooKeeper.getData(propPath, zkWatchMgr, null);

      var props = new PropEncodingV1(r);
      log.trace("Props for {}, returning {}", propPath, props);

      return props;

    } catch (KeeperException ex) {
      throw new IllegalStateException("Could not get properties for " + propPath, ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted getting properties for " + propPath, ex);
    }
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
    log.info("Setting props at {}", propPath);
    try {
      Stat stat = zooKeeper.exists(propPath, false);
      if (Objects.isNull(stat)) {
        var name =
            zooKeeper.create(propPath, props.toBytes(), ZooUtil.PUBLIC, CreateMode.PERSISTENT);
        log.trace("Created zooKeeper node: {}", name);
        return;
      }

      // Stat x = zooKeeper.exists(propPath, false);

      var zkExpectedVersion = props.getDataVersion();

      log.debug("would like to update: {}", ZooUtil.printStat(stat));
      log.debug("update is version: {} - {} ", zkExpectedVersion, props.print(true));

      zooKeeper.setData(propPath, props.toBytes(), zkExpectedVersion);

    } catch (KeeperException ex) {
      throw new IllegalStateException("Could not set properties for " + propPath, ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted setting properties for " + propPath, ex);
    }
  }

  /**
   * Convert the table configuration properties from individual zooKeeper nodes (Accumulo pre-2.1
   * format) into the single node (2.1) format. If the destination configuration node exists, it
   * will be overwritten.
   *
   * @param srcPath
   *          the parent path to current configuration nodes (i.e
   *          /accumulo/[instance]/[tables|namespace]/id
   * @param destId
   *          the CacheId for the destination.
   * @throws KeeperException
   *           if a zooKeeper exception occurs
   * @throws InterruptedException
   *           if an interrupt occurs.
   */
  public void upgrade(final String srcPath, final CacheId destId)
      throws KeeperException, InterruptedException {

    String destPath = String.format("%s/%s", configRoot, destId.nodeName());

    Stat srcStat = zooKeeper.exists(srcPath, false);

    if (Objects.nonNull(srcStat)) {

      PropEncoding props = convert(srcPath, srcStat.getPzxid());

      Stat destStat = zooKeeper.exists(destPath, false);

      byte[] payload = props.toBytes();

      log.info("Props: {}", props.print(true));

      if (Objects.isNull(destStat)) {
        zooKeeper.create(destPath, payload, ZooUtil.PRIVATE, CreateMode.PERSISTENT);
      } else {
        zooKeeper.setData(destPath, payload, destStat.getVersion());
      }
    }
  }

  /**
   * Read the properties for a table in zooKeeper under
   * /accumulo/[instance_id/[tables|namespace]/[id]/conf and convert them to PropEncoded instance.
   *
   * @param srcPath
   *          the path in zooKeeper with configuration properties
   * @param pzxid
   *          the zooKeeper version id of the conf node.
   * @return an PropEncoded instance
   * @throws KeeperException
   *           is a zooKeeper exception occurs
   * @throws InterruptedException
   *           if an interrupt is received.
   */
  private PropEncoding convert(final String srcPath, final long pzxid)
      throws KeeperException, InterruptedException {

    Map<String,Stat> nodeVersions = new HashMap<>();

    PropEncoding props = new PropEncodingV1(1, true, Instant.now());

    List<String> configNodes = zooKeeper.getChildren(srcPath, false);

    for (String propName : configNodes) {
      var cStat = new Stat();
      var path = srcPath + "/" + propName;
      byte[] data = zooKeeper.getData(path, null, cStat);
      props.addProperty(propName, new String(data, UTF_8));
      nodeVersions.put(path, cStat);
    }

    // validate that config has not changed while processing child nodes.
    Stat stat = zooKeeper.exists(srcPath, false);

    // This is checking for children additions / deletions and does not check data versions on the
    // child nodes.
    if (pzxid != stat.getPzxid()) {
      // this could also retry.
      throw new IllegalStateException(
          "PropCache number of nodes under " + srcPath + " changed while copying to new format.");
    }

    // Check the node version ids for a change.
    int changes = 0;
    for (Map.Entry<String,Stat> entry : nodeVersions.entrySet()) {
      stat = zooKeeper.exists(entry.getKey(), false);
      if (Objects.isNull(stat) || entry.getValue().getVersion() != stat.getVersion()) {
        log.debug("Path {} changed during upgrade", entry.getKey());
        changes++;
      }
    }

    if (changes > 0) {
      throw new IllegalStateException(
          "PropCache was modified, " + changes + " changes found during upgrade. ");
    } else {
      log.debug("Migrated {} properties to new storage format", nodeVersions.size());
    }

    return props;
  }

  /**
   * Convert the table configuration properties from a single node into multiple nodes used by
   * Accumulo versions less than 2.1.
   *
   * @param srcId
   *          The cache id of the source configuration
   * @param destPath
   *          the path to the destination parent node (i.e
   *          /accumulo/[instance_id/[tables|namespace]/[id]/conf)
   */
  public void downgrade(final CacheId srcId, final String destPath)
      throws KeeperException, InterruptedException {

    String srcPath = String.format("%s/%s", configRoot, srcId.nodeName());

    // read
    byte[] r = zooKeeper.getData(srcPath, false, null);

    PropEncoding props = new PropEncodingV1(r);

    Map<String,String> allProps = props.getAllProperties();

    Stat stat = zooKeeper.exists(destPath, false);
    if (Objects.isNull(stat)) {
      zooKeeper.create(destPath, null, ZooUtil.PRIVATE, CreateMode.PERSISTENT);
    }

    for (Map.Entry<String,String> p : allProps.entrySet()) {
      var nodePath = destPath + "/" + p.getKey();
      stat = zooKeeper.exists(nodePath, false);
      if (Objects.isNull(stat)) {
        zooKeeper.create(nodePath, p.getValue().getBytes(UTF_8), ZooUtil.PRIVATE,
            CreateMode.PERSISTENT);
      } else {
        zooKeeper.setData(nodePath, p.getValue().getBytes(UTF_8), stat.getVersion());
      }
    }
  }

  public static class Builder {

    private ZooKeeper zooKeeper;
    private String instanceId;
    private Ticker ticker;

    public ZooPropStore build() {

      Objects.requireNonNull(zooKeeper, "Valid ZooKeeper instance must be supplied");
      Objects.requireNonNull(instanceId, "Valid instance ID must be supplied");

      return new ZooPropStore(zooKeeper, instanceId);

    }

    public Builder withZk(final ZooKeeper zooKeeper) {
      this.zooKeeper = zooKeeper;
      return this;
    }

    public Builder forInstance(String instanceID) {
      this.instanceId = instanceID;
      return this;
    }

    public Builder usingTestCache(final Ticker ticker) {
      this.ticker = ticker;
      return this;
    }

  }
}
