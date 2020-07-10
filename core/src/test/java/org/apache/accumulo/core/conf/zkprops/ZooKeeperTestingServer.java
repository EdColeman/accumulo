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
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses Apache Curator to create a running zookeeper server for internal tests. The zookeeper port
 * is randomly assigned in case multiple instances are created by concurrent tests.
 */
public class ZooKeeperTestingServer {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperTestingServer.class);
  private static final Random rand = new SecureRandom();
  private final ZooKeeper zoo;
  private TestingServer zkServer;

  /**
   * Instantiate a running zookeeper server - this call will block until the server is ready for
   * client connections. It will try three times, with a 5 second pause to connect.
   */
  public ZooKeeperTestingServer() {
    this(getPort());
  }

  private ZooKeeperTestingServer(int port) {

    try {

      Path tmpDir = Files.createTempDirectory("zk_test");

      CountDownLatch connectionLatch = new CountDownLatch(1);

      // using a random port. The test server allows for auto port
      // generation, but not with specifying the tmp dir path too.
      // so, generate our own.
      boolean started = false;
      int retry = 0;
      while (!started && retry++ < 3) {

        try {

          zkServer = new TestingServer(port, tmpDir.toFile());
          zkServer.start();

          started = true;
        } catch (Exception ex) {
          log.trace("zookeeper test server start failed attempt {}", retry);
        }
      }

      log.info("zookeeper connection string:'{}'", zkServer.getConnectString());

      zoo = new ZooKeeper(zkServer.getConnectString(), 5_000, watchedEvent -> {
        if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
          connectionLatch.countDown();
        }
      });

      connectionLatch.await();

    } catch (Exception ex) {
      throw new IllegalStateException("Failed to start testing zookeeper", ex);
    }

  }

  /**
   * Returns an random integer between 50_000 and 65_000 (typical ephemeral port range for linux is
   * listed as 49,152 to 65,535
   *
   * @return a random port with the linux ephemeral port range.
   */
  private static int getPort() {
    final int minPort = 50_000;
    final int maxPort = 65_000;
    return rand.nextInt((maxPort - minPort) + 1) + minPort;
  }

  public static String prettyStat(final Stat stat) {

    if (stat == null) {
      return "{Stat:[null]}";
    }

    return "{Stat:[" + "czxid:" + stat.getCzxid() + ", mzxid:" + stat.getMzxid() + ", ctime: "
        + stat.getCtime() + ", mtime: " + stat.getMtime() + ", version: " + stat.getVersion()
        + ", cversion: " + stat.getCversion() + ", aversion: " + stat.getAversion()
        + ", eph owner: " + stat.getEphemeralOwner() + ", dataLength: " + stat.getDataLength()
        + ", numChildren: " + stat.getNumChildren() + ", pzxid: " + stat.getPzxid() + "}";

  }

  public ZooKeeper getZooKeeper() {
    return zoo;
  }

  public String getConn() {
    return zkServer.getConnectString();
  }

  public void initPaths(String s) {
    try {

      String[] paths = s.split("/");

      String slash = "/";
      String path = "";

      for (String p : paths) {
        if (!p.isEmpty()) {
          path = path + slash + p;

          try {
            Stat info = zoo.exists(path, false);
            if (Objects.isNull(info)) {
              log.debug("initializing paths, creating node {}", path);
              zoo.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
          } catch (KeeperException.NodeExistsException ex) {
            log.trace("initializing paths, skipping node {} it already exists", path);
          }
        }
      }

    } catch (Exception ex) {
      throw new IllegalStateException("Failed to create accumulo initial paths: " + s, ex);
    }
  }

  public void close() throws IOException {
    if (zkServer != null) {
      zkServer.stop();
    }
  }
}
