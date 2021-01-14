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
package org.apache.accumulo.server.conf2;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooPropStore implements PropStore {

  private static final Logger log = LoggerFactory.getLogger(ZooPropStore.class);

  private final ZooKeeper zookeeper;
  private final ZkNotificationManager zkWatcher;

  private final String configRoot;

  private final AtomicBoolean zkConnected = new AtomicBoolean(false);

  public ZooPropStore(final ZooKeeper zookeeper, final String instanceId) {

    this.zookeeper = zookeeper;
    this.configRoot = String.format("/accumulo/%s/config2", instanceId);

    this.zkWatcher = new ZkNotificationManager(zookeeper, this, configRoot);

  }

  @Override
  public PropEncoding get(CacheId id, Notifier notifier) {

    // if (Objects.nonNull(pcl)) {
    // addPropertyChangeListener(pcl);
    // }

    var propPath = String.format("%s/%s", configRoot, id.asKey());

    try {
      Stat stat = zookeeper.exists(propPath, false);
      if (Objects.isNull(stat)) {
        // no config node - create node with empty props
        return new PropEncodingV1(1, true, Instant.now());
      }
      // read
      byte[] r = zookeeper.getData(propPath, false, null);

      return new PropEncodingV1(r);

    } catch (KeeperException ex) {
      throw new IllegalStateException("Could not get properties for " + propPath, ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted getting properties for " + propPath, ex);
    }
  }

  @Override
  public void set(CacheId id, PropEncoding props) {}

  @Override
  public void registerForChanges(Notifier notifier) {

  }

  @Override
  public boolean isReady() {
    return zkConnected.get();
  }

  @Override
  public void enable() {
    zkConnected.set(true);
  }

  @Override
  public void disable() {
    zkConnected.set(false);
  }

  /**
   * Convert the table configuration properties from individual ZooKeeper nodes (Accumulo pre-2.1
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

    String destPath = String.format("%s/%s", configRoot, destId.asKey());

    Stat srcStat = zookeeper.exists(srcPath, false);

    if (Objects.nonNull(srcStat)) {

      PropEncoding props = convert(srcPath, srcStat.getPzxid());

      Stat destStat = zookeeper.exists(destPath, false);

      byte[] payload = props.toBytes();

      log.info("Props: {}", props.print(true));

      if (Objects.isNull(destStat)) {
        zookeeper.create(destPath, payload, ZooUtil.PRIVATE, CreateMode.PERSISTENT);
      } else {
        zookeeper.setData(destPath, payload, destStat.getVersion());
      }
    }
  }

  /**
   * Read the properties for a table in zookeeper under
   * /accumulo/[instance_id/[tables|namespace]/[id]/conf and convert them to PropEncoded instance.
   *
   * @param srcPath
   *          the path in zookeeper with configuration properties
   * @param pzxid
   *          the zookeeper version id of the conf node.
   * @return an PropEncoded instance
   * @throws KeeperException
   *           is a zookeeper exception occurs
   * @throws InterruptedException
   *           if an interrupt is received.
   */
  private PropEncoding convert(final String srcPath, final long pzxid)
      throws KeeperException, InterruptedException {

    Map<String,Stat> nodeVersions = new HashMap<>();

    PropEncoding props = new PropEncodingV1(1, true, Instant.now());

    List<String> configNodes = zookeeper.getChildren(srcPath, false);

    for (String propName : configNodes) {
      var cStat = new Stat();
      var path = srcPath + "/" + propName;
      byte[] data = zookeeper.getData(path, null, cStat);
      props.addProperty(propName, new String(data, UTF_8));
      nodeVersions.put(path, cStat);
    }

    // validate that config has not changed while processing child nodes.
    Stat stat = zookeeper.exists(srcPath, false);

    // This is checking for children additions / deletions and does not check data versions on the
    // child nodes.
    if (pzxid != stat.getPzxid()) {
      // this could also retry.
      throw new IllegalStateException(
          "PropCache number of nodes under " + srcPath + " changed while coping to new format.");
    }

    // Check the node version ids for a change.
    int changes = 0;
    for (Map.Entry<String,Stat> entry : nodeVersions.entrySet()) {
      stat = zookeeper.exists(entry.getKey(), false);
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

    String srcPath = String.format("%s/%s", configRoot, srcId.asKey());

    // read
    byte[] r = zookeeper.getData(srcPath, false, null);

    PropEncoding props = new PropEncodingV1(r);

    Map<String,String> allProps = props.getAllProperties();

    Stat stat = zookeeper.exists(destPath, false);
    if (Objects.isNull(stat)) {
      zookeeper.create(destPath, null, ZooUtil.PRIVATE, CreateMode.PERSISTENT);
    }

    for (Map.Entry<String,String> p : allProps.entrySet()) {
      var nodePath = destPath + "/" + p.getKey();
      stat = zookeeper.exists(nodePath, false);
      if (Objects.isNull(stat)) {
        zookeeper.create(nodePath, p.getValue().getBytes(UTF_8), ZooUtil.PRIVATE,
            CreateMode.PERSISTENT);
      } else {
        zookeeper.setData(nodePath, p.getValue().getBytes(UTF_8), stat.getVersion());
      }
    }
  }
}
