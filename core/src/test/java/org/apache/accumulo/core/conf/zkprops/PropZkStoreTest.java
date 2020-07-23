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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.Constants;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropZkStoreTest {

  private static final String INSTANCE = "1234";
  public static final String ZK_PROPS_BASE = Constants.ZROOT + "/" + INSTANCE + "/props";

  public static final String ZK_SENTINEL_ROOT = ZK_PROPS_BASE + "/sentinel";
  public static final String ZK_SYSTEM_PROPS_PATH = ZK_PROPS_BASE + "/system";
  public static final String ZK_NS_PROPS_BASE = ZK_PROPS_BASE + "/namespace/";
  public static final String ZK_TABLE_PROPS_BASE = ZK_PROPS_BASE + "/table/";;
  private static final Logger log = LoggerFactory.getLogger(PropZkStoreTest.class);
  private static ZooKeeperTestingServer szk = null;

  @BeforeClass
  public static void setupZk() {
    // using default zookeeper port - we don't have a full configuration
    szk = new ZooKeeperTestingServer();

    szk.initPaths(ZK_PROPS_BASE);
    szk.initPaths(ZK_SENTINEL_ROOT);
    szk.initPaths(ZK_SYSTEM_PROPS_PATH);
    szk.initPaths(ZK_NS_PROPS_BASE);
    szk.initPaths(ZK_TABLE_PROPS_BASE);

  }

  @AfterClass
  public static void shutdownZK() throws Exception {
    szk.close();
  }

  @Test
  public void emptyStore() {
    PropStore store = new PropZkStore(szk.getZooKeeper());
    CacheablePropMap data = store.get(ZkPropPath.of("/accumulo/unknown"));
  }

  @Test
  public void simpleStore() throws Exception {

    PropStore store = new PropZkStore(szk.getZooKeeper());
    var tablePath = ZkPropPath.of(ZK_TABLE_PROPS_BASE + "table1");

    CacheablePropMap data = store.get(tablePath);
    assertNull(data);

    store.setProperty(PropId.Scope.TABLE, tablePath, "aProp", "aValue");

    Stat s = szk.getZooKeeper().exists(tablePath.canonical(), false);

    data = store.get(tablePath);

    assertNotNull(data);

    log.info("Stat: {}", ZooKeeperTestingServer.prettyStat(s));
  }

  /**
   * This test simulates concurrent modifications to the underlying props by directly using
   * zookeeper between updates. This test may need rework if watchers are implemented.
   *
   * @throws Exception
   *           is an error occurs
   */
  @Test
  public void forceInvalidCache() throws Exception {

    PropStore store = new PropZkStore(szk.getZooKeeper());
    var tablePath = ZkPropPath.of(ZK_TABLE_PROPS_BASE + "table1");

    CacheablePropMap data = store.get(tablePath);
    assertNull(data);

    store.setProperty(PropId.Scope.TABLE, tablePath, "aProp", "aValue");

    Stat s = szk.getZooKeeper().exists(tablePath.canonical(), false);

    data = store.get(tablePath);

    assertNotNull(data);

    int cacheInvalidVerCount = ((PropZkStore) store).getCacheInvalidVerCount();

    log.info("Data: {}", data);
    log.info("Stat: {}", ZooKeeperTestingServer.prettyStat(s));

    // force a change in the zookeeper node.
    Stat stat = new Stat();
    byte[] bytes = szk.getZooKeeper().getData(tablePath.canonical(), false, stat);
    stat = szk.getZooKeeper().setData(tablePath.canonical(), bytes, stat.getVersion());

    log.info("Forced update: {}", ZooKeeperTestingServer.prettyStat(stat));
    store.setProperty(PropId.Scope.TABLE, tablePath, "bProp", "bValue");

    assertTrue(((PropZkStore) store).getCacheInvalidVerCount() > cacheInvalidVerCount);

    log.info("Data: {}", data);
    log.info("Stat: {}", ZooKeeperTestingServer.prettyStat(s));

  }
}
