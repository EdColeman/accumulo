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

import org.apache.accumulo.core.Constants;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PropZkStoreTest {

  private static final String INSTANCE = "1234";
  public static final String ZK_PROPS_BASE = Constants.ZROOT + "/" + INSTANCE + "/props";

  public static final String ZK_SENTINEL_ROOT = ZK_PROPS_BASE + "/sentinel";
  public static final String ZK_SYSTEM_PROPS_PATH = ZK_PROPS_BASE + "/system";
  public static final String ZK_NS_PROPS_BASE = ZK_PROPS_BASE + "/namespace/";
  public static final String ZK_TABLE_PROPS_BASE = ZK_PROPS_BASE + "/table/";
  private static final Logger log = LoggerFactory.getLogger(PropZkStoreTest.class);
  private static ZooKeeperTestingServer szk = null;

  @BeforeClass public static void setupZk() {
    // using default zookeeper port - we don't have a full configuration
    szk = new ZooKeeperTestingServer();

    szk.initPaths(ZK_PROPS_BASE);
    szk.initPaths(ZK_SENTINEL_ROOT);
    szk.initPaths(ZK_SYSTEM_PROPS_PATH);
    szk.initPaths(ZK_NS_PROPS_BASE);
    szk.initPaths(ZK_TABLE_PROPS_BASE);

  }

  @AfterClass public static void shutdownZK() throws Exception {
    szk.close();
  }

  @Test public void emptyStore() {
    PropStore store = new PropZkStore(szk.getZooKeeper());
    Map<String,String> data = store.getAll(ZkPropPath.of("/accumulo/unknown"));
  }

  @Test public void simpleStore() throws Exception {

    PropStore store = new PropZkStore(szk.getZooKeeper());
    var tablePath = ZkPropPath.of(ZK_TABLE_PROPS_BASE + "table1");

    Map<String,String> data = store.getAll(tablePath);
    assertNull(data);

    // store.setProperty(PropId.Scope.TABLE, tablePath, "aProp", "aValue");
    store.setProperty(tablePath, "aProp", "aValue");

    Stat s = szk.getZooKeeper().exists(tablePath.canonical(), false);

    data = store.getAll(tablePath);

    assertNotNull(data);

    log.info("Stat: {}", ZooKeeperTestingServer.prettyStat(s));
  }

  /**
   * This test simulates concurrent modifications to the underlying props by directly using
   * zookeeper between updates. This test may need rework if watchers are implemented.
   *
   * @throws Exception is an error occurs
   */
  @Test public void forceInvalidCache() throws Exception {

    PropStore store = new PropZkStore(szk.getZooKeeper());
    var tablePath = ZkPropPath.of(ZK_TABLE_PROPS_BASE + "table1");

    Map<String,String> data = store.getAll(tablePath);
    assertNull(data);

    store.setProperty(tablePath, "aProp", "aValue");

    Stat s = szk.getZooKeeper().exists(tablePath.canonical(), false);

    data = store.getAll(tablePath);

    assertNotNull(data);

    int cacheInvalidVerCount = ((PropZkStore) store).getCacheInvalidVerCount();

    log.info("Data: {}", data);
    log.info("Stat: {}", ZooKeeperTestingServer.prettyStat(s));

    // force a change in the zookeeper node.
    Stat stat = new Stat();
    byte[] bytes = szk.getZooKeeper().getData(tablePath.canonical(), false, stat);
    stat = szk.getZooKeeper().setData(tablePath.canonical(), bytes, stat.getVersion());

    log.info("Forced update: {}", ZooKeeperTestingServer.prettyStat(stat));
    store.setProperty(tablePath, "bProp", "bValue");

    log.debug("Version conflicts: {}", ((PropZkStore) store).getCacheInvalidVerCount());

    assertTrue(((PropZkStore) store).getCacheInvalidVerCount() > cacheInvalidVerCount);

    log.info("Data: {}", data);
    log.info("Stat: {}", ZooKeeperTestingServer.prettyStat(s));

  }

  @Test public void getTest() {
    PropStore store = new PropZkStore(szk.getZooKeeper());
    var tablePath = ZkPropPath.of(ZK_TABLE_PROPS_BASE + "table1");

    Map<String,String> data = store.getAll(tablePath);
    assertNull(data);

    store.setProperty(tablePath, "aProp", "aValue");

    Optional<String> v = store.getProperty(tablePath, "aProp");
    if(v.isPresent()){
      assertEquals("aValue", v.get());
    } else {
      fail("expected value not present");
    }
  }

  @Test public void getMissingPropTest() {
    PropStore store = new PropZkStore(szk.getZooKeeper());
    var tablePath = ZkPropPath.of(ZK_TABLE_PROPS_BASE + "table1");

    Map<String,String> data = store.getAll(tablePath);
    assertNull(data);

    store.setProperty(tablePath, "aProp", "aValue");

    Optional<String> v = store.getProperty(tablePath, "zProp");
    if(v.isPresent()) {
      fail("expected value no value to be present");
    }

  }
  @Test(expected = UnsupportedOperationException.class)  public void deleteTest() {
    PropStore store = new PropZkStore(szk.getZooKeeper());
    var tablePath = ZkPropPath.of(ZK_TABLE_PROPS_BASE + "table1");

    Map<String,String> data = store.getAll(tablePath);
    assertNull(data);

    store.setProperty(tablePath, "aProp", "aValue");

    store.deleteProp(tablePath, "aProp");
  }

  @Test public void deleteAllTest() {
    PropStore store = new PropZkStore(szk.getZooKeeper());
    var tablePath = ZkPropPath.of(ZK_TABLE_PROPS_BASE + "table1");

    Map<String,String> data = store.getAll(tablePath);
    assertNull(data);

    store.setProperty(tablePath, "aProp", "aValue");

    store.deleteAll(tablePath);
  }

  @Test(expected = UnsupportedOperationException.class) public void clonePropertiesTest() {
    PropStore store = new PropZkStore(szk.getZooKeeper());
    var tablePath = ZkPropPath.of(ZK_TABLE_PROPS_BASE + "table1");
    var clonePath = ZkPropPath.of(ZK_TABLE_PROPS_BASE + "copy_table1");

    Map<String,String> data = store.getAll(tablePath);
    assertNull(data);

    store.setProperty(tablePath, "aProp", "aValue");

    store.cloneProperties(tablePath, clonePath);
  }

  @Test(expected = UnsupportedOperationException.class) public void setPropertiesTest() {
    PropStore store = new PropZkStore(szk.getZooKeeper());
    var tablePath = ZkPropPath.of(ZK_TABLE_PROPS_BASE + "table1");

    Map<String,String> data = store.getAll(tablePath);
    assertNull(data);

    List<Map.Entry<String,String>> list = new ArrayList<>();

    store.setProperties(list);
  }
}
