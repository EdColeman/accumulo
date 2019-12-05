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
package org.apache.accumulo.server.conf.propstore.proto2;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.server.conf.propstore.ZooFunc.prettyStat;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf.propstore.ZookeeperTestingServer;
import org.apache.accumulo.server.conf.propstore.dummy.NodeData;
import org.apache.accumulo.server.conf.propstore.dummy.ZkOps;
import org.apache.accumulo.server.conf.propstore.dummy.ZkOpsImpl;
import org.apache.accumulo.server.conf.propstore.proto2.cache.PropMap;
import org.apache.accumulo.server.conf.propstore.proto2.cache.PropWriter;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SentinelMonitorTest {

  private static final Logger log = LoggerFactory.getLogger(SentinelMonitorTest.class);

  private static ZookeeperTestingServer testingServer;
  private static ZkOps zoo;

  private static final String rootPath = "/accumulo/1234/config";
  private static final String nodePath = rootPath + "/node-000001";

  public static final TableId TABLE_1_ID = TableId.of("a123");

  @BeforeClass
  public static void setup() {
    testingServer = new ZookeeperTestingServer();
    zoo = new ZkOpsImpl(testingServer.getZooKeeper());

    testingServer.initPaths(rootPath);
  }

  @AfterClass
  public static void teardown() throws IOException {
    testingServer.close();
  }

  @Test
  public void go() throws Exception {

    SentinelMonitor monitor = new SentinelMonitor(nodePath, null, zoo);
    log.debug("monitor: {}", monitor);

    // validate that monitor created the node.
    Stat s = testingServer.getZooKeeper().exists(nodePath, false);

    assertNotNull(s);

    log.info("stat: {}", prettyStat(s));

    Thread.sleep(500);

    PropWriter writer = new PropWriter(zoo);

    Stat data1 = new Stat();
    data1.setVersion(-1);

    PropMap m = new PropMap(TableId.of("abc1"), data1);

    writer.updateAll(nodePath, TableId.of("abc1"), m);

    // zoo.writeProps(nodePath, TableId.of("abc1"), m);

    // simulate external node data change.
    Thread.sleep(500);
    // testingServer.getZooKeeper().setData(nodePath, new byte[0], -1);

    NodeData reason = zoo.readNodeData(nodePath, null);
    log.debug("reason: {}", new String(reason.getData(), UTF_8));
  }
}
