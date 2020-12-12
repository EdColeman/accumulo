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

import org.apache.accumulo.core.data.TableId;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ZooPropStoreTest {

  private static final Logger log = LoggerFactory.getLogger(ZooPropStoreTest.class);
  private static transient boolean haveZookeeper = false;
  private static ZooKeeper zookeeper;
  private final String unoInstId = "2b0234bb-c157-4de5-bed0-0446528f50e8";

  @BeforeClass public static void init() {
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

  @AfterClass public static void close() {
    if (haveZookeeper) {
      try {
        zookeeper.close();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Test public void simpleStore() {

    Assume.assumeTrue("Could not connect to zookeeper, skipping", haveZookeeper);

    // zookeeper.exists("", false);

  }

  @Test public void upgradeTest() throws Exception {

    Assume.assumeTrue("Could not connect to zookeeper, skipping", haveZookeeper);

    ZooPropStore propStore = new ZooPropStore(zookeeper, unoInstId);
    propStore.upgrade(TableId.of("2"));

    String destPath = String.format("/accumulo/%s/tables/2/conf2", unoInstId);

    // read
    byte[] r = zookeeper.getData(destPath, false, null);

    PropEncoding props2 = new PropEncodingV1(r);
    log.info("Props2: {}", props2.print(true));

    propStore.downgrade(TableId.of("2"));

  }

  private static class SessionWatcher implements Watcher {

    private static final Logger log = LoggerFactory.getLogger(SessionWatcher.class);

    @Override public void process(WatchedEvent watchedEvent) {
      log.debug("Received session event {}", watchedEvent);
    }
  }
}
