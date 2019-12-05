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
package org.apache.accumulo.server.conf.propstore;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.accumulo.core.data.TableId;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooFuncTest {

  private static final Logger log = LoggerFactory.getLogger(ZooFuncTest.class);

  private static ZookeeperTestingServer testingServer;
  private static ZooKeeper zoo;

  private static final String NODE_1_PATH = "/accumulo/1234/config/node-000001";
  public static final TableId TABLE_1_ID = TableId.of("a123");

  @BeforeClass
  public static void setup() {
    testingServer = new ZookeeperTestingServer();
    zoo = testingServer.getZooKeeper();

    testingServer.initPaths(NODE_1_PATH);
  }

  @AfterClass
  public static void teardown() throws IOException {
    testingServer.close();
  }

  /**
   * Validate connection valid using a test zookeeper instance.
   *
   * @throws KeeperException
   *           any exception is a test failure
   * @throws InterruptedException
   *           any exception is a test failure
   */
  @Test
  public void connectEventTest() throws KeeperException, InterruptedException {

    log.info("Z:{}", testingServer);

    testingServer.initPaths(NODE_1_PATH);

    assertTrue(ZooFunc.waitForZooConnection(zoo));

  }

  /**
   * Validate that a connection timeout will throw an IllegalStateException using a mocked
   * zookeeper.
   *
   * @throws Exception
   *           any excption other than IllegalStateException is a test failure.
   */
  @Test(expected = IllegalStateException.class)
  public void connectTimeoutTestMock() throws Exception {
    ZooKeeper mockZooKeeper = EasyMock.createStrictMock(ZooKeeper.class);

    // first call will check for /accumulo/[instance]
    EasyMock.expect(mockZooKeeper.getState()).andAnswer(() -> ZooKeeper.States.CONNECTING)
        .anyTimes();

    EasyMock.replay(mockZooKeeper);

    // expecting IllegalStateException
    new SentinelNode(NODE_1_PATH, mockZooKeeper);
  }
}
