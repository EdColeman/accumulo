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
package org.apache.accumulo.test.config2;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.accumulo.server.conf2.impl.GuavaPropStore;
import org.apache.accumulo.test.config2.cluster.ExpectedValues;
import org.apache.accumulo.test.config2.cluster.PropReader;
import org.apache.accumulo.test.config2.cluster.TestWorker;
import org.apache.accumulo.test.config2.cluster.ZkAgitator;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiZooConfigTest {

  public static final int TEST_RUN_TIME_MILLIS = 60_000;
  private static final Logger log = LoggerFactory.getLogger(MultiZooConfigTest.class);
  private static final int NUM_ZK_SERVERS = 3;
  private static final int NUM_SIMULATED_TABLES = 2;
  private static final byte[] FAKE_AUTH = "accumulo:uno".getBytes(UTF_8);
  private static final String INSTANCE_ID = UUID.randomUUID().toString();
  private static final String configPath =
      Constants.ZROOT + "/" + INSTANCE_ID + Constants.ZENCODED_CONFIG_ROOT;
  private static final ExecutorService pool = Executors.newWorkStealingPool(4);
  private static TestingCluster zkTestCluster;
  private static ZooKeeper zoo;

  private static ExpectedValues truth;
  private static ServerContext mockContext = null;

  @BeforeClass
  public static void init() throws Exception {

    var zkSpecs = TestingCluster.makeSpecs(NUM_ZK_SERVERS);

    // create cluster
    zkTestCluster = new TestingCluster(zkSpecs);

    try {
      zkTestCluster.start();
    } catch (IllegalStateException ex) {
      try {
        zkTestCluster.close();
      } catch (Exception e) {
        // ignore
      }
      zkTestCluster = new TestingCluster(NUM_ZK_SERVERS);
      zkTestCluster.start();
    }

    // create ZooKeeper client.
    String connectString = zkTestCluster.getConnectString();

    log.info("Connecting to: {}", connectString);
    CountDownLatch connectionLatch = new CountDownLatch(1);

    zoo = new ZooKeeper(connectString, 5_000, watchedEvent -> {
      if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
        connectionLatch.countDown();
      }
    });

    connectionLatch.await();

    createDefaultPaths();

    // tests only need context to return instance id.
    mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();
    replay(mockContext);

    truth = new ExpectedValues(mockContext, NUM_SIMULATED_TABLES);

    var store = new GuavaPropStore.Builder().withZk(zoo).forInstance(INSTANCE_ID).buildGuavaCache();
    var ids = truth.getIds();

    Map<String,String> values = new HashMap<>();
    values.put(ExpectedValues.PROP_INT_NAME, "-1");

    PropEncoding prop = new PropEncodingV1();
    prop.addProperties(values);

    ids.forEach(id -> {
      store.writeToStore(id, prop);
    });
  }

  private static void createDefaultPaths() {

    log.debug("Creating default config path for test: {}", configPath);

    try {

      zoo.addAuthInfo("digest", FAKE_AUTH);

      String[] paths = configPath.split("/");

      String slash = "/";
      String path = "";

      for (String p : paths) {
        if (!p.isEmpty()) {
          path = path + slash + p;
          log.debug("building default paths, creating node {}", path);
          zoo.create(path, null, ZooUtil.PUBLIC, CreateMode.PERSISTENT);
        }
      }

    } catch (Exception ex) {
      throw new IllegalStateException("Failed to create accumulo initial paths: " + configPath, ex);
    }
  }

  @AfterClass
  public static void stopZooCluster() throws Exception {
    log.info("****** test shutdown ******");
    pool.shutdownNow();
    zkTestCluster.close();
  }

  @Test
  public void numServersTest() throws Exception {

    ServerContext mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();

    replay(mockContext);

    final CacheId testId = CacheId.forTable(mockContext, TableId.of("abc1"));

    log.info("None: {}", zkTestCluster.getInstances());

    zoo.addAuthInfo("digest", FAKE_AUTH);

    InstanceSpec connectionInstance = zkTestCluster.findConnectionInstance(zoo);

    log.info("Connected to {}", connectionInstance);
    log.info("Node: {} exists: {}", configPath, zoo.exists(configPath, new TestWatcher()));

    TestWorker worker1 =
        new PropReader(zkTestCluster.getConnectString(), "reader-1", INSTANCE_ID, truth);
    pool.submit(worker1);

    TestWorker worker2 = new ZkAgitator(zkTestCluster, "worker-2");
    pool.submit(worker2);

    Thread.sleep(TEST_RUN_TIME_MILLIS);
    log.info("Zoo state: {}", zoo.getState());
    log.info("Exists: {}", zoo.exists(configPath, false));
    log.info("Connected to {}", zkTestCluster.findConnectionInstance(zoo));

    worker1.quit();

    zoo.close();
  }

  private static class TestWatcher implements Watcher {

    @Override
    public void process(WatchedEvent watchedEvent) {
      log.info("*** Event ***\nReceived event: {}", watchedEvent);
    }
  }

}
