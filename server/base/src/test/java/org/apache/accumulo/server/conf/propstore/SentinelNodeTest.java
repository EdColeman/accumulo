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

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO - tests disabled while refactoring, need to move to different classes that will have the functionality.
public class SentinelNodeTest {

  private static final Logger log = LoggerFactory.getLogger(SentinelNodeTest.class);

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

  public void getEmptyTablePropsMock() throws KeeperException, InterruptedException {
    ZooKeeper mockZooKeeper = EasyMock.createStrictMock(ZooKeeper.class);

    // first call will check for connection
    EasyMock.expect(mockZooKeeper.getState()).andAnswer(() -> ZooKeeper.States.CONNECTED);

    // next call will check for /accumulo/[instance]/config/node exits.
    EasyMock.expect(mockZooKeeper.exists(EasyMock.isA(String.class), EasyMock.anyBoolean()))
        .andAnswer(Stat::new);

    // next call will call getData to set watcher on /accumulo/[instance]/config/node1
    EasyMock.expect(mockZooKeeper.getData(EasyMock.isA(String.class), EasyMock.isA(Watcher.class),
        EasyMock.isA(Stat.class))).andAnswer(() -> new byte[0]);

    // next call will call getData to read table properties
    // /accumulo/[instance]/config/node1/[tableId]
    EasyMock.expect(mockZooKeeper.getData(EasyMock.isA(String.class), EasyMock.anyBoolean(),
        EasyMock.isA(Stat.class))).andAnswer(() -> new byte[0]);

    EasyMock.replay(mockZooKeeper);

    // SentinelNode node = new SentinelNode(NODE_1_PATH, mockZooKeeper);

    // ZkMap m = node.getTableProps(TABLE_1_ID);
    // log.info("getEmptyTableProps: N:{}, ZkMap:{}", node, m);

    // confirm that the data was cached - no zookeeper call should be made.
    // ZkMap m2 = node.getTableProps(TableId.of("a123"));

    // log.info("getEmptyTableProps: N:{}, ZkMap:{}", node, m2);
  }

  public void getEmptyTableProps() throws KeeperException, InterruptedException {
    log.info("Z:{}", testingServer);

    // SentinelNode node = new SentinelNode(NODE_1_PATH, testingServer.getZooKeeper());

    // ZkMap m = node.getTableProps(TableId.of("a123"));
    // log.info("getEmptyTableProps: N:{}, ZkMap:{}", node, m);

  }

  public void getTablePropsMock() throws KeeperException, InterruptedException {

    byte[] data = getTable1Bytes();
    ZooKeeper mockZooKeeper = EasyMock.createStrictMock(ZooKeeper.class);

    // first call will check for connection
    EasyMock.expect(mockZooKeeper.getState()).andAnswer(() -> ZooKeeper.States.CONNECTED);

    // next call will check for /accumulo/[instance]/config/node exits.
    EasyMock.expect(mockZooKeeper.exists(EasyMock.isA(String.class), EasyMock.anyBoolean()))
        .andAnswer(Stat::new);

    // next call will call getData to set watcher on /accumulo/[instance]/config/node1
    EasyMock.expect(mockZooKeeper.getData(EasyMock.isA(String.class), EasyMock.isA(Watcher.class),
        EasyMock.isA(Stat.class))).andAnswer(() -> new byte[0]);

    // next call will call getData to read table properties
    // /accumulo/[instance]/config/node1/[tableId]
    EasyMock.expect(mockZooKeeper.getData(EasyMock.isA(String.class), EasyMock.anyBoolean(),
        EasyMock.isA(Stat.class))).andAnswer(() -> data);

    EasyMock.replay(mockZooKeeper);

    // SentinelNode node = new SentinelNode(NODE_1_PATH, mockZooKeeper);

    // ZkMap r = node.getTableProps(TABLE_1_ID);
    // log.info("getTableProps: N:{}, ZkMap:{}", node, r);
  }

  public void getTableProp() throws KeeperException, InterruptedException {

    log.info("Z:{}", testingServer);

    zoo.create(NODE_1_PATH + "/" + TABLE_1_ID.canonical(), getTable1Bytes(),
        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    // SentinelNode node = new SentinelNode(NODE_1_PATH, testingServer.getZooKeeper());

    // ZkMap m = node.getTableProps(TABLE_1_ID);

    // log.info("tableProps: N:{}, ZkMap:{}", node, m);

  }

  public void processUpdateMock() throws InterruptedException, KeeperException {

    byte[] data = getTable1Bytes();
    ZooKeeper mockZooKeeper = EasyMock.createStrictMock(ZooKeeper.class);

    // first call will check for connection
    EasyMock.expect(mockZooKeeper.getState()).andAnswer(() -> ZooKeeper.States.CONNECTED);

    // next call will check for /accumulo/[instance]/config/node exits.
    EasyMock.expect(mockZooKeeper.exists(EasyMock.isA(String.class), EasyMock.anyBoolean()))
        .andAnswer(Stat::new);

    // next call will call getData to set watcher on /accumulo/[instance]/config/node1
    EasyMock.expect(mockZooKeeper.getData(EasyMock.isA(String.class), EasyMock.isA(Watcher.class),
        EasyMock.isA(Stat.class))).andAnswer(() -> new byte[0]);

    // next call will call getData to read table properties
    // /accumulo/[instance]/config/node1/[tableId]
    EasyMock.expect(mockZooKeeper.getData(EasyMock.isA(String.class), EasyMock.anyBoolean(),
        EasyMock.isA(Stat.class))).andAnswer(() -> data);

    ZkMap u = ZkMap.fromJson(data);
    u.set(Property.TABLE_BLOOM_ENABLED, "false");

    EasyMock.replay(mockZooKeeper);

    // SentinelNode node = new SentinelNode(NODE_1_PATH, mockZooKeeper);

    // ZkMap r = node.getTableProps(TABLE_1_ID);
    // log.info("getTableProps: N:{}, ZkMap:{}", node, r);
  }

  public void setTableProp() throws KeeperException, InterruptedException {

    log.info("Z:{}", testingServer);

    // SentinelNode node = new SentinelNode(NODE_1_PATH, testingServer.getZooKeeper());

    // ZkMap b = node.getTableProps(TABLE_1_ID);

    // log.info("tableProps: N:{}, ZkMap:{}", node, b);

    ZkMap m = new ZkMap(TABLE_1_ID, -99);
    m.set(Property.TABLE_FILE_MAX, "777");
    // node.setTableProps(m);

    try {
      Thread.sleep(200);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    // ZkMap u1 = node.getTableProps(TABLE_1_ID);

    // trigger update
    m.set(Property.TABLE_BLOOM_ENABLED, "true");
    m.set(Property.TABLE_FILE_MAX, "123");
    // node.setTableProps(m);

    try {
      Thread.sleep(200);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    // ZkMap u2 = node.getTableProps(TABLE_1_ID);

    // log.info("tableProps: Before:{}, ZkMap:{}", node, u1);
    // log.info("tableProps: Update:{}, ZkMap:{}", node, u2);

  }

  /**
   * Generate a byte[] of json encoded table properties.
   *
   * @return json encoded table props
   */
  private byte[] getTable1Bytes() {
    ZkMap m = new ZkMap(TABLE_1_ID, 123);

    m.set(Property.TABLE_BLOOM_ENABLED, "true");
    m.set(Property.TABLE_FILE_MAX, "99");

    return m.toJson();
  }

  @Test
  public void x() {
    Pattern p = Pattern.compile("^/(.+/)*(.+)$");

    Matcher m = p.matcher("/a/b");

    log.info("N-1:{} - {}", m.matches(), m.group(m.groupCount()));

    m = p.matcher("/a");

    log.info("N-2:{} - {}", m.matches(), m.group(m.groupCount()));

    m = p.matcher("/accumulo/1234/config/node1");

    log.info("N-3:{} - {}", m.matches(), m.group(m.groupCount()));

    m = p.matcher("/a/b-123");

    log.info("N-4:{} - {}", m.matches(), m.group(m.groupCount()));

  }
}
