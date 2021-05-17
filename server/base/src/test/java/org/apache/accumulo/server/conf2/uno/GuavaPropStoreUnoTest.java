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
package org.apache.accumulo.server.conf2.uno;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuavaPropStoreUnoTest {

  private static final Logger log = LoggerFactory.getLogger(GuavaPropStoreUnoTest.class);
  private static transient boolean haveZookeeper = false;
  private static ZooKeeper zookeeper;
  private final String unoInstId = "2b0234bb-c157-4de5-bed0-0446528f50e8";
  WatchLogger watcher = new WatchLogger();

  @BeforeClass
  public static void init() {
    try {
      CountDownLatch connectionLatch = new CountDownLatch(1);
      zookeeper = new ZooKeeper("localhost:2181", 10_000, watchedEvent -> {
        if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
          connectionLatch.countDown();
        }
      });

      if (!connectionLatch.await(10, TimeUnit.SECONDS)) {
        log.info("failed tp get zookeeper connected event");
      }

      if (zookeeper.getState() == ZooKeeper.States.CONNECTED) {
        haveZookeeper = true;
      }
      zookeeper.addAuthInfo("digest", ("accumulo:uno").getBytes(UTF_8));

    } catch (IOException | InterruptedException ex) {
      log.info("Failed to connect to zookeeper - these tests should be skipped.", ex);
      haveZookeeper = false;
    }
  }

  @AfterClass
  public static void close() {
    if (haveZookeeper) {
      try {
        zookeeper.close();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Test
  public void simpleStore() {

    Assume.assumeTrue("Could not connect to zookeeper, skipping", haveZookeeper);

    // zookeeper.exists("", false);

  }

  @Test
  public void sessionTest() throws Exception {

    // Assume.assumeTrue("Could not connect to zookeeper, skipping", haveZookeeper);
    //
    // MemPropStore store = new MemPropStore();
    // String configRoot = "/accumulo/" + unoInstId + Constants.ZENCODED_CONFIG_ROOT;
    //
    // ZkNotificationManager notifier = new ZkNotificationManager(configRoot, zookeeper, store);
    //
    // Stat s = zookeeper.exists(configRoot, notifier);
    //
    // try {
    //
    // Thread.sleep(1_000);
    // WchcCommandTest wchc = new WchcCommandTest();
    // wchc.watcherSnapshot();
    //
    // Thread.sleep(60_000);
    // } catch (InterruptedException ex) {
    // // empty
    // }
  }

  @Test
  public void captureEvents() {
    Assume.assumeTrue("Could not connect to zookeeper, skipping", haveZookeeper);

    try {

      var rootPath = "/dummy";
      var pNodeName = rootPath + "/dummy-1234-p";
      var eNodeName = rootPath + "/dummy-1234-e";

      String name = zookeeper.create(eNodeName, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL_SEQUENTIAL);
      zookeeper.exists(name, watcher);

      try {
        zookeeper.create(pNodeName, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } catch (KeeperException.NodeExistsException ex) {
        // empty
      }

      List<String> children = zookeeper.getChildren("/dummy", false);
      var nodePath = rootPath + "/" + children.get(0);

      log.debug("nodeName: {}, Nodes: {}", pNodeName, children);

      Stat s = zookeeper.exists(pNodeName, watcher);
      log.debug("Node stat: {}", s);

      zookeeper.setData(pNodeName, "data".getBytes(UTF_8), -1);

      // reset watcher
      s = zookeeper.exists(pNodeName, watcher);
      log.debug("Node stat: {}", s);

      log.info("sleeping...");
      Thread.sleep(60_000);

    } catch (KeeperException | InterruptedException ex) {
      log.info("Failed: ", ex);
    }

  }

  private static class WatchLogger implements Watcher {

    private static final Logger log = LoggerFactory.getLogger(WatchLogger.class);

    @Override
    public void process(WatchedEvent watchedEvent) {
      log.warn("** Received session event {}", watchedEvent);
    }
  }
}
