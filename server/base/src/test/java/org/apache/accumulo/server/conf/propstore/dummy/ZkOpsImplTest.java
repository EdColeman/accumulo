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
package org.apache.accumulo.server.conf.propstore.dummy;

import java.io.IOException;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf.propstore.ZookeeperTestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkOpsImplTest {

  private static final Logger log = LoggerFactory.getLogger(ZkOpsImpl.class);

  private static ZookeeperTestingServer testingServer;
  private static ZooKeeper zoo;

  private static final String rootPath = "/accumulo/1234/config";
  private static final String nodePath = rootPath + "/node-000001";

  public static final TableId TABLE_1_ID = TableId.of("a123");

  @BeforeClass
  public static void setup() {
    testingServer = new ZookeeperTestingServer();
    zoo = testingServer.getZooKeeper();

    testingServer.initPaths(rootPath);
  }

  @AfterClass
  public static void teardown() throws IOException {
    testingServer.close();
  }

  @Test
  public void getNodeStat() throws Exception {

    Watcher watcher = new ZkWatcher();

    ZkOps zoo = new ZkOpsImpl(testingServer.getZooKeeper());

    Stat stat = zoo.getNodeStat(nodePath, watcher);

    log.info("Stat: {}", stat);

    Thread.sleep(2_000);

    testingServer.getZooKeeper().setData(nodePath, new byte[0], -1);

    Thread.sleep(2_000);
  }

  @Test
  public void writeProps() {

  }

  private static class ZkWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      log.info("Received event: {}", event);
      if (event.getType().equals(Event.EventType.NodeCreated)) {
        try {
          testingServer.getZooKeeper().exists(event.getPath(), this);
        } catch (KeeperException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
