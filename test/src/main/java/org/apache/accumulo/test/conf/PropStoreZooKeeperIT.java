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
package org.apache.accumulo.test.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf2.PropCacheId;
import org.apache.accumulo.server.conf2.PropChangeListener;
import org.apache.accumulo.server.conf2.PropStoreException;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.accumulo.server.conf2.impl.PropStoreFactory;
import org.apache.accumulo.server.conf2.impl.ZooPropStore;
import org.apache.accumulo.test.categories.ZooKeeperTestingServerTests;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ZooKeeperTestingServerTests.class})
public class PropStoreZooKeeperIT {

  public static final String INSTANCE_ID = UUID.randomUUID().toString();
  public static final String ZK_CONFIG_ROOT =
      "/accumulo/" + INSTANCE_ID + Constants.ZENCODED_CONFIG_ROOT;
  private static final Logger log = LoggerFactory.getLogger(PropStoreZooKeeperIT.class);
  private static ZooKeeperTestingServer testZk = null;
  private static ZooKeeper zooKeeper;

  @BeforeClass
  public static void setupZk() {
    // using default zookeeper port - we don't have a full configuration
    testZk = new ZooKeeperTestingServer("test");
    zooKeeper = testZk.getZooKeeper();
  }

  @AfterClass
  public static void shutdownZK() throws Exception {
    testZk.close();
  }

  @Before
  public void setupZnodes() {
    testZk.initPaths(ZK_CONFIG_ROOT);
  }

  @After
  public void cleanupZnodes() {
    try {
      ZKUtil.deleteRecursive(zooKeeper, "/accumulo");
    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to clean-up test zooKeeper nodes.", ex);
    }
  }

  @Test
  public void init() throws InterruptedException {

    log.info("Initialized zookeeper paths with default sys props: {}",
        ZooPropStore.initNode(INSTANCE_ID, zooKeeper, PropCacheId.forSystem(INSTANCE_ID), null));

    ZooPropStore propStore =
        (ZooPropStore) new PropStoreFactory().forInstance(INSTANCE_ID).withZk(zooKeeper).build();

    Map<String,String> fixed = propStore.readFixed();
    assertNotNull(fixed);

    Thread.sleep(2_000);
  }

  @Test
  public void createNoProps() throws PropStoreException {
    ZooPropStore propStore =
        (ZooPropStore) new PropStoreFactory().forInstance(INSTANCE_ID).withZk(zooKeeper).build();

    PropCacheId tableA = PropCacheId.forTable(INSTANCE_ID, TableId.of("A"));
    propStore.create(tableA, null);

    assertNotNull(propStore.get(tableA));
  }

  @Test
  public void createWithProps() throws PropStoreException {
    ZooPropStore propStore =
        (ZooPropStore) new PropStoreFactory().forInstance(INSTANCE_ID).withZk(zooKeeper).build();

    PropCacheId tableA = PropCacheId.forTable(INSTANCE_ID, TableId.of("A"));
    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(tableA, initialProps);

    assertNotNull(propStore.get(tableA));
    assertEquals("true", propStore.get(tableA).getProperty(Property.TABLE_BLOOM_ENABLED.getKey()));
  }

  @Test
  public void update() throws PropStoreException, InterruptedException {

    ZooPropStore propStore =
        (ZooPropStore) new PropStoreFactory().forInstance(INSTANCE_ID).withZk(zooKeeper).build();

    TestChangeListener listener = new TestChangeListener();

    PropCacheId tableA = PropCacheId.forTable(INSTANCE_ID, TableId.of("A"));
    propStore.registerAsListener(tableA, listener);

    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(tableA, initialProps);

    assertNotNull(propStore.get(tableA));
    assertEquals("true", propStore.get(tableA).getProperty(Property.TABLE_BLOOM_ENABLED.getKey()));

    int version0 = propStore.get(tableA).getDataVersion();

    Map<String,String> updateProps = new HashMap<>();
    updateProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "false");
    updateProps.put(Property.TABLE_MAJC_RATIO.getKey(), "5");

    propStore.update(tableA, updateProps);

    // allow change notification to propagate
    Thread.sleep(10);

    // validate version changed on write.
    int version1 = propStore.get(tableA).getDataVersion();
    assertTrue(version0 < version1);

    assertNotNull(propStore.get(tableA));
    assertEquals(2, propStore.get(tableA).getAllProperties().size());
    assertEquals("false", propStore.get(tableA).getProperty(Property.TABLE_BLOOM_ENABLED.getKey()));
    assertEquals("5", propStore.get(tableA).getProperty(Property.TABLE_MAJC_RATIO.getKey()));

    propStore.removeProperties(tableA,
        Collections.singletonList(Property.TABLE_MAJC_RATIO.getKey()));

    // validate version changed on write
    int version2 = propStore.get(tableA).getDataVersion();
    assertTrue(version0 < version2);
    assertTrue(version1 < version2);

    // allow change to propagate
    Thread.sleep(10);

    assertEquals(1, propStore.get(tableA).getAllProperties().size());
    assertEquals("false", propStore.get(tableA).getProperty(Property.TABLE_BLOOM_ENABLED.getKey()));
    assertNull(propStore.get(tableA).getProperty(Property.TABLE_MAJC_RATIO.getKey()));

    assertEquals(2, (int) listener.getChangeCounts().get(tableA));
    assertNull(listener.getDeleteCounts().get(tableA));

  }

  @Test
  public void delete() throws PropStoreException, InterruptedException {
    ZooPropStore propStore =
        (ZooPropStore) new PropStoreFactory().forInstance(INSTANCE_ID).withZk(zooKeeper).build();

    TestChangeListener listener = new TestChangeListener();

    PropCacheId tableA = PropCacheId.forTable(INSTANCE_ID, TableId.of("A"));
    PropCacheId tableB = PropCacheId.forTable(INSTANCE_ID, TableId.of("B"));

    propStore.registerAsListener(tableA, listener);
    propStore.registerAsListener(tableB, listener);

    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(tableA, initialProps);
    propStore.create(tableB, initialProps);

    assertNotNull(propStore.get(tableA));
    assertNotNull(propStore.get(tableB));
    assertEquals("true", propStore.get(tableA).getProperty(Property.TABLE_BLOOM_ENABLED.getKey()));

    // use 3nd prop store - change will propagate via ZooKeeper
    ZooPropStore propStore2 =
        (ZooPropStore) new PropStoreFactory().forInstance(INSTANCE_ID).withZk(zooKeeper).build();
    propStore2.delete(tableA);

    try {
      // allow zk event to propagate
      Thread.sleep(50);
      assertNull(propStore.get(tableA));
      fail("Expected PropStoreException to occur on non-existent table");
    } catch (PropStoreException ex) {
      assertEquals(PropStoreException.REASON_CODE.NO_ZK_NODE, ex.getCode());
    }

    assertNotNull(propStore.get(tableB));

    // validate change count not triggered
    assertNull(listener.getChangeCounts().get(tableA));
    assertNull(listener.getChangeCounts().get(tableB));

    // validate delete only for table A
    assertEquals(1, (int) listener.getDeleteCounts().get(tableA));
    assertNull(listener.getChangeCounts().get(tableA));

  }

  /**
   * Simulate change in props by process external to the prop store instance.
   */
  @Test
  public void externalChange() throws PropStoreException, InterruptedException, KeeperException {

    ZooPropStore propStore =
        (ZooPropStore) new PropStoreFactory().forInstance(INSTANCE_ID).withZk(zooKeeper).build();

    TestChangeListener listener = new TestChangeListener();

    PropCacheId tableA = PropCacheId.forTable(INSTANCE_ID, TableId.of("A"));
    PropCacheId tableB = PropCacheId.forTable(INSTANCE_ID, TableId.of("B"));

    propStore.registerAsListener(tableA, listener);
    propStore.registerAsListener(tableB, listener);

    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(tableA, initialProps);
    propStore.create(tableB, initialProps);

    assertNotNull(propStore.get(tableA));
    assertNotNull(propStore.get(tableB));

    PropEncoding readProps = propStore.get(tableA);

    assertEquals("true", readProps.getProperty(Property.TABLE_BLOOM_ENABLED.getKey()));

    // This assumes default is resolved at a higher level
    assertNull(readProps.getProperty(Property.TABLE_BLOOM_SIZE.getKey()));

    Map<String,String> update = new HashMap<>();
    var bloomSize = "1_000_000";
    update.put(Property.TABLE_BLOOM_SIZE.getKey(), bloomSize);
    readProps.addProperties(update);

    byte[] updatedBytes = readProps.toBytes();
    // force external write to ZooKeeper
    zooKeeper.setData(tableA.path(), updatedBytes, readProps.getExpectedVersion());

    Thread.sleep(200);
    PropEncoding updatedProps = propStore.get(tableA);

    assertNotNull(readProps.getProperty(Property.TABLE_BLOOM_SIZE.getKey()));
    assertEquals(bloomSize, readProps.getProperty(Property.TABLE_BLOOM_SIZE.getKey()));

    assertNotNull(updatedProps.getProperty(Property.TABLE_BLOOM_SIZE.getKey()));
    assertEquals(bloomSize, updatedProps.getProperty(Property.TABLE_BLOOM_SIZE.getKey()));

    log.info("Prop changes {}", listener.getChangeCounts());
    log.info("Prop deletes {}", listener.getDeleteCounts());

  }

  private static class TestChangeListener implements PropChangeListener {

    private final Map<PropCacheId,Integer> changeCounts = new ConcurrentHashMap<>();
    private final Map<PropCacheId,Integer> deleteCounts = new ConcurrentHashMap<>();

    @Override
    public void changeEvent(PropCacheId id) {
      changeCounts.merge(id, 1, Integer::sum);
    }

    @Override
    public void deleteEvent(PropCacheId id) {
      deleteCounts.merge(id, 1, Integer::sum);
    }

    public Map<PropCacheId,Integer> getChangeCounts() {
      return changeCounts;
    }

    public Map<PropCacheId,Integer> getDeleteCounts() {
      return deleteCounts;
    }
  }

  @Test
  public void dvTest() {
    PropEncoding p1 = new PropEncodingV1();
    log.info("p1: {}", p1.print(true));
    p1.toBytes();
    log.info("p2: {}", p1.print(true));
    p1.toBytes();
    log.info("p3: {}", p1.print(true));
  }
}
