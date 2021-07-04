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
package org.apache.accumulo.server.conf2.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.PropCacheId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert pre-2.1 system, namespace and table properties to PropEncoded format.
 *
 * <pre>
 * Source ZooKeeper paths:
 *   srcSysPath - system config source = /accumulo/[iid-id]/config;
 *   srcNsBasePath - namespace config source /accumulo/[iid]/namespaces;
 *   srcTableBasePath - table config source /accumulo/[iid]/tables;
 * </pre>
 */
public class ConfigurationUpgrade {

  private static final Logger log = LoggerFactory.getLogger(ConfigurationUpgrade.class);

  private final ServerContext context;
  private final ZooKeeper zooKeeper;
  private final String instanceId;
  private final String zkBasePath; // base path for accumulo instance - /accumulo/[iid]
  private final String destBasePath; // path for new config -/accumulo/[iid]/ZENCODED_CONFIG_ROOT

  public ConfigurationUpgrade(final ServerContext context, final boolean deleteWhenComplete) {

    this.context = context;

    try {
      this.zooKeeper = getZooClient(context.getZooKeepers());
    } catch (IOException ex) {
      throw new IllegalStateException("Failed to connect to ZooKeeper", ex);
    }

    this.instanceId = context.getInstanceID();

    zkBasePath = Constants.ZROOT + "/" + instanceId;
    destBasePath = String.format("/accumulo/%s%s", instanceId, Constants.ZENCODED_CONFIG_ROOT);

  }

  public ConfigurationUpgrade(final ServerContext context, final ZooKeeper zooKeeper,
      final boolean deleteWhenComplete) {

    this.context = context;
    this.zooKeeper = zooKeeper;

    this.instanceId = context.getInstanceID();

    zkBasePath = Constants.ZROOT + "/" + instanceId;
    destBasePath = String.format("/accumulo/%s%s", instanceId, Constants.ZENCODED_CONFIG_ROOT);
  }

  public ConfigurationUpgrade(final ServerContext context) {
    this(context, false);
  }

  private ZooKeeper getZooClient(final String zooConnectString) throws IOException {

    ZooKeeper zoo;

    try {
      CountDownLatch connectionLatch = new CountDownLatch(1);
      zoo = new ZooKeeper(zooConnectString, 5_000, watchedEvent -> {
        if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
          connectionLatch.countDown();
        }
      });

      // ZooSession.getAuthenticatedSession(conf.get(Property.INSTANCE_ZK_HOST),
      // (int) conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT), "digest",
      // ("accumulo" + ":" + conf.get(Property.INSTANCE_SECRET)).getBytes(UTF_8));

      connectionLatch.await();

    } catch (IOException ex) {
      throw new IllegalStateException("Failed to connect to ZooKeeper", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted connecting to ZooKeeper", ex);
    }

    // var secret = context.getConfiguration().get(Property.INSTANCE_SECRET);

    zoo.addAuthInfo("digest",
        ("accumulo" + ":" + context.getConfiguration().get(Property.INSTANCE_SECRET))
            .getBytes(UTF_8));

    return zoo;
  }

  public boolean performUpgrade() {

    try {
      // system
      convertSystem();
      // namespaces
      convertNamespaces();
      // tables
      convertTables();

      // TODO - validate
      return verifyAll();

    } catch (KeeperException ex) {
      throw new IllegalStateException("Failed to complete configuration property upgrade", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted during configuration property upgrade", ex);
    }

  }

  public void convertSystem() throws KeeperException, InterruptedException {
    upgrade(getSrcSysPath(), PropCacheId.forSystem(context));
  }

  public void convertNamespaces() throws KeeperException, InterruptedException {

    List<String> namespaces = zooKeeper.getChildren(getSrcNsBasePath(), false);
    for (String ns : namespaces) {
      var nid = NamespaceId.of(ns);
      var path = getSrcNsPath(nid);
      log.info("Looking for: {}", path);
      upgrade(path, PropCacheId.forNamespace(context, nid));
    }
  }

  public void convertTables() throws KeeperException, InterruptedException {

    List<String> tables = zooKeeper.getChildren(getSrcTableBasePath(), false);
    for (String t : tables) {
      var tid = TableId.of(t);
      var path = getSrcTablePath(tid);
      log.info("Looking for: {}", path);
      upgrade(path, PropCacheId.forTable(context, tid));
    }
  }

  /**
   * Return the pre-2.1 path for system config info.
   *
   * @return the source path for system configuration
   */
  @SuppressWarnings("deprecation")
  public String getSrcSysPath() {
    return zkBasePath + Constants.ZCONFIG;
  }

  /**
   * Return the pre-2.1 base path for namespace info. General form is
   *
   * <pre>
   * "/accumulo/[inst-id]/namespaces
   * </pre>
   *
   * @return the parent node for namespace info
   */
  public String getSrcNsBasePath() {
    return zkBasePath + Constants.ZNAMESPACES;
  }

  /**
   * Return pre-2.1 path in zookeeper for namespace configuration nodes. General form is
   *
   * <pre>
   * /accumulo/[inst-id]/namespaces/[nid]/conf
   * </pre>
   *
   * @param nid
   *          a namespace id.
   * @return return the parent node for namespace configuration.
   */
  @SuppressWarnings("deprecation")
  public String getSrcNsPath(final NamespaceId nid) {
    return getSrcNsBasePath() + "/" + nid.canonical() + Constants.ZNAMESPACE_CONF;
  }

  /**
   * Return the pre-2.1 base path in zookeeper for table configuration - general form is
   *
   * <pre>
   * "/accumulo/[inst-id]/tables/[tid]/conf"
   * </pre>
   *
   * @return the base path in zookeeeper for table info
   */
  public String getSrcTableBasePath() {
    return zkBasePath + Constants.ZTABLES;
  }

  /**
   * Return the pre-2.1 base path in zookeeper for table configuration - general form is
   *
   * <pre>
   * "/accumulo/[inst-id]/tables/[tid]/conf"
   * </pre>
   *
   * @return the base path in zookeeeper for table indo
   */
  @SuppressWarnings("deprecation")
  public String getSrcTablePath(final TableId tid) {
    return getSrcTableBasePath() + "/" + tid.canonical() + Constants.ZTABLE_CONF;
  }

  /**
   * Upgrade the table configuration properties from individual zooKeeper nodes (Accumulo pre-2.1
   * format) into the single node (2.1) format. If the destination configuration node exists, it
   * will be overwritten.
   *
   * @param srcPath
   *          the parent path to current configuration nodes (i.e
   *          /accumulo/[instance]/[tables|namespace]/id
   * @param destId
   *          the PropCacheId1 for the destination.
   * @throws KeeperException
   *           if a zooKeeper exception occurs
   * @throws InterruptedException
   *           if an interrupt occurs.
   */
  public void upgrade(final String srcPath, final PropCacheId destId)
      throws KeeperException, InterruptedException {

    log.info("ConfigurationUpgrade - src: {}, id: {}", srcPath, destId);

    String destPath = destId.getPath();

    Stat srcStat = zooKeeper.exists(srcPath, false);

    if (Objects.nonNull(srcStat)) {

      PropEncoding props = encode(srcPath, srcStat.getPzxid());

      Stat destStat = zooKeeper.exists(destPath, false);

      byte[] payload = props.toBytes();

      log.info("Props: {}", props.print(true));

      if (Objects.isNull(destStat)) {
        log.info("ConfigurationUpgrade - create {}", destPath);
        log.info("Base: {} -> {}", destBasePath, zooKeeper.exists(destBasePath, false));
        zooKeeper.create(destPath, payload, ZooUtil.PRIVATE, CreateMode.PERSISTENT);
      } else {
        log.info("ConfigurationUpgrade - update {}", destPath);
        zooKeeper.setData(destPath, payload, destStat.getVersion());
      }
    }
  }

  /**
   * Read the properties for a table in zooKeeper under
   * /accumulo/[instance_id/[tables|namespace]/[id]/conf and encode them to PropEncoded instance.
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
  private PropEncoding encode(final String srcPath, final long pzxid)
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
      throw new IllegalStateException("ZooPropCache number of nodes under " + srcPath
          + " changed while copying to new format.");
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
          "ZooPropCache was modified, " + changes + " changes found during upgrade. ");
    } else {
      log.debug("Migrated {} properties to new storage format", nodeVersions.size());
    }

    return props;
  }

  public boolean verifyAll() {
    try {

      int convertedCount = 0;

      log.info("Verify system");
      // check system
      convertedCount++;
      if (!verifyById(PropCacheId.forSystem(context))) {
        return false;
      }

      // check namespace(s)
      log.info("Verify namespace(s)");
      List<String> namespaces = zooKeeper.getChildren(getSrcNsBasePath(), false);
      for (String ns : namespaces) {
        convertedCount++;
        var nid = NamespaceId.of(ns);
        var path = getSrcNsPath(nid);
        log.info("Looking for: {}", path);
        if (!verifyById(PropCacheId.forNamespace(context, nid))) {
          return false;
        }
      }

      // check tables
      log.info("Verify tables");

      List<String> tables = zooKeeper.getChildren(getSrcTableBasePath(), false);
      for (String t : tables) {
        convertedCount++;
        var tid = TableId.of(t);
        var path = getSrcTablePath(tid);
        log.info("Looking for: {}", path);
        if (!verifyById(PropCacheId.forTable(context, tid))) {
          return false;
        }
      }

      var numConverted = zooKeeper.getChildren(destBasePath, false).size();

      log.info("Converted {}, expected {}", convertedCount, numConverted);
      return convertedCount == numConverted;

    } catch (KeeperException ex) {
      throw new IllegalStateException("Verification failed with a ZooKeeper exception", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Verification failed with an interrupt", ex);
    }
  }

  public boolean verifyById(final PropCacheId id) {
    var idType = id.getIdType();

    switch (idType) {
      case SYSTEM:
        return verifyNode(id, getSrcSysPath());
      case NAMESPACE:
        return verifyNode(id, getSrcNsPath(id.getNamespaceId().orElseThrow(
            () -> new IllegalArgumentException("No namespace id set for id: " + id.toString()))));
      case TABLE:
        return verifyNode(id, getSrcTablePath(id.getTableId().orElseThrow(
            () -> new IllegalArgumentException("No table id id set for id: " + id.toString()))));
      case UNKNOWN:
        log.warn("Unknown type for id: {}", id);
        return false;
      default:
        log.warn("Undefined type for id: {}", id);
        return false;
    }
  }

  private boolean verifyNode(final PropCacheId id, final String srcPath) {
    log.info("verifyById {} converted to {}", srcPath, id);
    try {

      // check "empty" node - converted node does not exist,
      // fail if there are props that were not converted
      Stat convertedNode = zooKeeper.exists(id.getPath(), false);
      if (Objects.isNull(convertedNode)) {
        Stat p = zooKeeper.exists(srcPath, false);
        return Objects.isNull(p);
      }

      PropEncoding converted =
          new PropEncodingV1(zooKeeper.getData(id.getPath(), false, convertedNode));
      log.info("Stat before: {}", ZooUtil.printStat(convertedNode));
      log.info("P:{}", converted.getAllProperties());
      List<String> originalNames = zooKeeper.getChildren(srcPath, false);

      var convertedProps = converted.getAllProperties();
      if (convertedProps.size() != originalNames.size()) {
        log.warn("verification of system props failed, number of properties do not match");
        return false;
      }

      log.info("SIZE: {}", convertedProps.size());

      if (convertedProps.size() == 0) {
        return true;
      }

      for (Map.Entry<String,String> entry : convertedProps.entrySet()) {
        log.trace("Check: {}", entry);
        String v =
            new String(zooKeeper.getData(srcPath + "/" + entry.getKey(), false, null), UTF_8);
        if (!entry.getValue().equals(v)) {
          return false;
        }
      }

      return true;
      // check that
    } catch (Exception ex) {
      log.warn("ConfigurationUpgrade verification failed for node {} due to exception", id, ex);
      return false;
    }
  }

  /**
   * ConfigurationUpgrade the table configuration properties from a single node into multiple nodes
   * used by Accumulo versions less than 2.1.
   *
   * @param srcId
   *          The cache id of the source configuration
   * @param destPath
   *          the path to the destination parent node (i.e
   *          /accumulo/[instance_id/[tables|namespace]/[id]/conf)
   */
  public void downgrade(final PropCacheId srcId, final String destPath)
      throws KeeperException, InterruptedException {

    String srcPath = srcId.getPath();

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

}
