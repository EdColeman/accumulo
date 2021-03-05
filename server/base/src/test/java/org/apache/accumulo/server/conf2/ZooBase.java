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

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooBase {

  private static final Logger log = LoggerFactory.getLogger(ZooBase.class);

  private static transient boolean haveZookeeper = false;
  private static ZooKeeper zookeeper;

  private static final String uuid = UUID.randomUUID().toString();
  private static final String basePath = "/accumulo/" + uuid;

  @BeforeClass
  public static void init() throws Exception {
    try {
      CountDownLatch connectionLatch = new CountDownLatch(1);
      zookeeper = new ZooKeeper("localhost:2181", 10_000, watchedEvent -> {
        if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
          connectionLatch.countDown();
        }
      });

      if (!connectionLatch.await(10, TimeUnit.SECONDS)) {
        log.info("failed tp get zookeeper connected event");
        return;
      }

      if (zookeeper.getState() == ZooKeeper.States.CONNECTED) {
        haveZookeeper = true;
      }

      zookeeper.addAuthInfo("digest", ("accumulo:uno").getBytes(UTF_8));

      if (Objects.isNull(zookeeper.exists("/accumulo", false))) {
        zookeeper.create("/accumulo", null, ZooUtil.PUBLIC, CreateMode.PERSISTENT);
      }

      if (Objects.isNull(zookeeper.exists(getBasePath(), false))) {
        zookeeper.create(getBasePath(), null, ZooUtil.PUBLIC, CreateMode.PERSISTENT);
      }

      if (Objects.isNull(zookeeper.exists(getConfigPath(), false))) {
        zookeeper.create(getConfigPath(), null, ZooUtil.PUBLIC, CreateMode.PERSISTENT);
      }

    } catch (IOException | InterruptedException ex) {
      log.info("Failed to connect to zookeeper - these tests should be skipped.", ex);
      haveZookeeper = false;
    }
  }

  @AfterClass
  public static void close() throws Exception {
    if (haveZookeeper) {

      try {

        if (Objects.nonNull(zookeeper.exists(getConfigPath(), false))) {
          // clean-up: children /accumulo/[id]Constants.ZENCODED_CONFIG_ROOT/xxxxxx
          List<String> children = zookeeper.getChildren(getConfigPath(), false);
          children.forEach(c -> {
            try {
              var delPath = getConfigPath() + "/" + c;
              log.debug("cleanup. removing: {}", delPath);
              zookeeper.delete(delPath, -1);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
            } catch (KeeperException ex) {
              log.debug("Exception during zookeeper cleanup {}", c, ex);
            }
          });

          // clean-up: /accumulo/[id]+ Constants.ZENCODED_CONFIG_ROOT
          log.debug("cleanup removing: {}", getConfigPath());
          zookeeper.delete(getConfigPath(), -1);
        }

        if (Objects.nonNull(zookeeper.exists(getBasePath(), false))) {
          // clean-up: /accumulo/[id]
          log.debug("cleanup removing: {}", getBasePath());
          zookeeper.delete(getBasePath(), -1);
        }

        zookeeper.close();

      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static boolean haveZookeeper() {
    return haveZookeeper;
  }

  public static ZooKeeper getZooKeeper() {
    return zookeeper;
  }

  public static String getBasePath() {
    return basePath;
  }

  public static String getConfigPath() {
    return getBasePath() + Constants.ZENCODED_CONFIG_ROOT;
  }

  public static String getTestUuid() {
    return uuid;
  }

  // utility method to set some properties.
  public static void fillMap(final PropEncoding props) {
    props.addProperty("key1", "value1");
    props.addProperty("key2", "value2");
    props.addProperty("key3", "value3");
    props.addProperty("key4", "value4");
  }

}
