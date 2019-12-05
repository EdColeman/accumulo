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
package org.apache.accumulo.server.conf.propstore.proto2.cache;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf.propstore.TestingServerBase;
import org.apache.accumulo.server.conf.propstore.dummy.ZkOps;
import org.apache.accumulo.server.conf.propstore.dummy.ZkOpsImpl;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropWriterTest extends TestingServerBase {

  private static final Logger log = LoggerFactory.getLogger(PropWriterTest.class);

  private static final String NODE_1_PATH = "/accumulo/1234/config/node-000001";
  public static final TableId TABLE_1_ID = TableId.of("a123");

  @BeforeClass
  public static void init() {
    TestingServerBase.testingServer.initPaths(NODE_1_PATH);
  }

  @Test
  public void updateEmpty() throws Exception {

    String tablePath = NODE_1_PATH + '\'' + TABLE_1_ID.canonical();

    // ensure that no table props in zookeeper for this test.
    try {
      Stat s = zoo.exists(tablePath, false);
      if (s != null) {
        zoo.delete(tablePath, -1);
      }
    } catch (KeeperException ex) {
      ex.printStackTrace();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      return;
    }

    ZkOps zkOps = new ZkOpsImpl(zoo);

    PropWriter propWriter = new PropWriter(zkOps);

    propWriter.update(NODE_1_PATH, TABLE_1_ID, Property.TABLE_BLOOM_ENABLED, "true");

    Thread.sleep(200);

    PropMapZooLoader loader = new PropMapZooLoader(NODE_1_PATH, zkOps);

    PropMap m = loader.load(TABLE_1_ID);

    assertEquals("true", m.get(Property.TABLE_BLOOM_ENABLED).orElse(""));

  }

  @Test
  public void update() throws Exception {
    // use empty test to populate initial value.
    updateEmpty();

    ZkOps zkOps = new ZkOpsImpl(zoo);
    PropWriter propWriter = new PropWriter(zkOps);

    propWriter.update(NODE_1_PATH, TABLE_1_ID, Property.TABLE_FILE_MAX, "99");

    Thread.sleep(200);

    PropMapZooLoader loader = new PropMapZooLoader(NODE_1_PATH, zkOps);

    PropMap m = loader.load(TABLE_1_ID);

    assertEquals("99", m.get(Property.TABLE_FILE_MAX).orElse(""));
  }

  @Test
  public void end2end() {
    MutableStat stat = new MutableStat("name", "desc", "sample", "value");
    stat.add(1L);
    log.debug("s: {}", stat.lastStat());
  }
}
