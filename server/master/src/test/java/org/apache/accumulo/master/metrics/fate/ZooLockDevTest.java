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
package org.apache.accumulo.master.metrics.fate;

import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * Test FATE metrics using stubs and in-memory version of supporting infrastructure - a test
 * zookeeper server is used an the FATE repos are stubs, but this should represent the metrics
 * collection execution without needed to stand up a mini cluster to exercise these execution paths.
 */
public class ZooLockDevTest {

  public static final String INSTANCE_ID = "1234";
  public static final String MOCK_ZK_ROOT = "/accumulo/" + INSTANCE_ID;
  public static final String A_FAKE_SECRET = "aPasswd";
  private static final Logger log = LoggerFactory.getLogger(ZooLockDevTest.class);
  private static ZooKeeperTestingServer szk = null;
  private final MetricsSystem ms = DefaultMetricsSystem.initialize("Accumulo");
  private ZooStore<Master> zooStore = null;
  private ZooKeeper zookeeper = null;

  @BeforeClass public static void setupZk() {
    // using default zookeeper port - we don't have a full configuration
    szk = new ZooKeeperTestingServer();
    szk.initPaths(MOCK_ZK_ROOT);

    // populate ZooReaderWriter cache for future ZooReaderWriter.getInstance() calls
    ZooReaderWriter.getInstance(szk.getConn(), 30_000, A_FAKE_SECRET);
  }

  @AfterClass public static void shutdownZK() throws Exception {
    szk.close();
  }

  private static int compareBySeqId(String lPath, String rPath) {
    int lSeq = lPath.lastIndexOf('-');
    if (lSeq == -1) {
      throw new IllegalArgumentException(
          "Left side, not a valid ephemeral sequence '" + lPath + "'");
    }

    int rSeq = rPath.lastIndexOf('-');
    if (rSeq == -1) {
      throw new IllegalArgumentException(
          "Right side, not a valid ephemeral sequence '" + lPath + "'");
    }

    int r = lPath.substring(lSeq + 1, lPath.length())
        .compareTo(rPath.substring(rSeq + 1, rPath.length()));

    if (r != 0) {
      return r;
    }

    return lPath.compareTo(rPath);

  }

  /**
   * Instantiate a test zookeeper and setup mocks for Master and Context. The test zookeeper is used
   * create a ZooReaderWriter. The zookeeper used in tests needs to be the one from the
   * zooReaderWriter, not the test server, because the zooReaderWriter sets up ACLs.
   *
   * @throws Exception any exception is a test failure.
   */
  @Before public void init() throws Exception {
    zookeeper = ZooReaderWriter.getInstance().getZooKeeper();

    // szk.initPaths(MOCK_ZK_ROOT);

  }

  @Test public void simpleCreate() throws Exception {

    Random r = SecureRandom.getInstance("SHA1PRNG");

    Stat stat = new Stat();

    //    String node = String.format("%s-%08x-", "my-lock", r.nextInt());
    //    String path = String.format("%s/%s", MOCK_ZK_ROOT, node);
    //
    //    String name = zookeeper.create(path,null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

    String name = createLock(MOCK_ZK_ROOT, "my-lock");
    String name2 = createLock(MOCK_ZK_ROOT, "my-lock");

    List<String> children = zookeeper.getChildren(MOCK_ZK_ROOT, false);

    log.info("Created: {}", name);
    log.info("Size: {}: {}", children.size(), children);

  }

  String createLock(final String lockPath, final String lockPrefix)
      throws KeeperException, InterruptedException {

    Random r = new SecureRandom();

    String node = String.format("%s-%08x-", lockPrefix, r.nextInt());
    String path = String.format("%s/%s", lockPath, node);

    String name =
        zookeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

    // if the node created has the lowest ephemeral sequence id -
    List<String> children = zookeeper.getChildren(MOCK_ZK_ROOT, false);
    children.sort(ZooLockDevTest::compareBySeqId);

    log.info("Children: {}", children);


    assertTrue(children.get(0).startsWith(node));
    return name;
  }

  String prettyStat(final Stat stat) {

    if (stat == null) {
      return "{Stat:[null]}";
    }

    return "{Stat:[" + "czxid:" + stat.getCzxid() + ", mzxid:" + stat
        .getMzxid() + ", ctime: " + stat.getCtime() + ", mtime: " + stat
        .getMtime() + ", version: " + stat.getVersion() + ", cversion: " + stat
        .getCversion() + ", aversion: " + stat.getAversion() + ", eph owner: " + stat
        .getEphemeralOwner() + ", dataLength: " + stat.getDataLength() + ", numChildren: " + stat
        .getNumChildren() + ", pzxid: " + stat.getPzxid() + "}";

  }

  private void clear(String path) throws Exception {

    log.debug("clean up - search: {}", path);

    List<String> children = zookeeper.getChildren(path, false);

    if (children.isEmpty()) {
      log.debug("clean up - delete path: {}", path);

      if (!path.equals(MOCK_ZK_ROOT)) {
        zookeeper.delete(path, -1);
      }
      return;
    }

    for (String cp : children) {
      clear(path + "/" + cp);
    }
  }
}
