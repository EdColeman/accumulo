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
package org.apache.accumulo.test.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses Apache Curator to create a running zookeeper server for internal tests. The zookeeper port
 * is randomly assigned in case multiple instances are created by concurrent tests.
 */
public class ZooKeeperTestingServer implements AutoCloseable {

  public static final byte[] FAKE_AUTH = "accumulo:uno".getBytes(UTF_8);
  private static final Logger log = LoggerFactory.getLogger(ZooKeeperTestingServer.class);

  private TestingServer zkServer;
  private final ZooKeeper zoo;

  private static final Random rand = new SecureRandom();

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

      zoo.addAuthInfo("digest", FAKE_AUTH);

    } catch (Exception ex) {
      throw new IllegalStateException("Failed to start testing zookeeper", ex);
    }

  }

  /**
   * Add authorization to the ZooKeeper connection that simulates Accumulo ZooKeeper auths (models
   * uno)
   *
   * @param zoo
   *          a ZooKeeper connection.
   */
  public static void addDefaultAuth(final ZooKeeper zoo) {
    zoo.addAuthInfo("digest", FAKE_AUTH);
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

  public ZooKeeper getZooKeeper() {
    return zoo;
  }

  public String getConn() {
    return zkServer.getConnectString();
  }

  public void initPaths(String rootPath) {

    try {

      log.debug("Initialing zookeeper paths from root: {}", rootPath);

      zoo.addAuthInfo("digest", FAKE_AUTH);

      String[] paths = rootPath.split("/");

      String slash = "/";
      String path = "";

      for (String p : paths) {
        if (!p.isEmpty()) {
          path = path + slash + p;
          log.debug("building default paths, creating node {}", path);
          try {
            zoo.create(path, null, ZooUtil.PUBLIC, CreateMode.PERSISTENT);
          } catch (KeeperException.NodeExistsException ex) {
            log.info("Node :{} existed - not created", path);
          }
        }
      }

    } catch (Exception ex) {
      throw new IllegalStateException("Failed to create accumulo initial paths: " + rootPath, ex);
    }
  }

  @Override
  public void close() throws IOException {
    if (zkServer != null) {
      zkServer.stop();
    }
  }

}
