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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.PropCacheException;
import org.apache.accumulo.server.conf2.PropWatcher;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.accumulo.server.conf2.impl.ZooPropStore;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.cli.StatPrinter;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooPropStoreTest {

  public static final String INSTANCE_ID = UUID.randomUUID().toString();
  public static final String ZK_CONFIG_ROOT =
      "/accumulo/" + INSTANCE_ID + Constants.ZENCODED_CONFIG_ROOT;
  private static final Logger log = LoggerFactory.getLogger(ZooPropStoreTest.class);
  private static ZooKeeperTestingServer szk = null;
  private static ZooKeeper zooKeeper;

  private ZooPropStore store;

  @BeforeClass
  public static void setupZk() {
    // using default zookeeper port - we don't have a full configuration
    szk = new ZooKeeperTestingServer();

    zooKeeper = szk.getZooKeeper();

  }

  @AfterClass
  public static void shutdownZK() throws Exception {
    szk.close();
  }

  @Before
  public void setupZnodes() throws Exception {
    szk.initPaths(ZK_CONFIG_ROOT);

    store = new ZooPropStore.Builder().withZk(zooKeeper).forInstance(INSTANCE_ID).build();
  }

  @After
  public void removeZnodes() {

    try {

      ZKUtil.deleteRecursive(zooKeeper, "/accumulo");

    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to clean-up test zooKeeper nodes.", ex);
    }
  }

  /**
   * Simple write and then read props test.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void simpleWriteRead() throws Exception {

    CacheId id = writeToStore(TableId.of("a"));

    // check initial data version matches expectation
    Stat stat = zooKeeper.exists(ZK_CONFIG_ROOT + "/" + id.nodeName(), false);
    if (log.isDebugEnabled()) {
      log.debug("Stat:\n{}", printStat(stat));
    }

    assertEquals(0, stat.getVersion());

    PropEncoding readProps = store.readFromStore(id);

    assertEquals(0, readProps.getDataVersion());

  }

  /**
   * Validate data update - expected that a read after the update received the changed values and
   * that notification of data change event is received by the store client.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void updateTest() throws Exception {

    CacheId id = writeToStore(TableId.of("a"));

    MyPropWatchListener watcher = new MyPropWatchListener();

    store.register(watcher);

    PropEncoding readProps = store.readFromStore(id);
    readProps.addProperty("key99", "value_99");

    log.debug("Hello from thread: {}", Thread.currentThread().getName());

    store.writeToStore(id, readProps);

    PropEncoding readProps2 = store.readFromStore(id);

    assertEquals(1, readProps2.getDataVersion());

    Thread.sleep(500);

    assertEquals(1, watcher.getCounts(id));

  }

  public CacheId writeToStore(TableId tableId) {

    CacheId id = new CacheId(INSTANCE_ID, null, tableId);

    PropEncoding storedProps = createPropData();
    store.writeToStore(id, storedProps);

    return id;
  }

  // utility method to set some properties.
  public PropEncoding createPropData() {

    PropEncoding props = new PropEncodingV1();
    props.addProperty("key1", "value1");
    props.addProperty("key2", "value2");
    props.addProperty("key3", "value3");
    props.addProperty("key4", "value4");

    return props;
  }

  private String printStat(final Stat stat) {

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos, true, UTF_8.name())) {
      StatPrinter sp = new StatPrinter(ps);
      sp.print(stat);
      return baos.toString(UTF_8);
    } catch (IOException ex) {
      log.debug("Exception trying to print stat: {}", stat, ex);
    }
    // failed formatting, return default.
    return stat.toString();
  }

  @Test(expected = PropCacheException.class)
  public void setInvalidProp() throws PropCacheException {
    CacheId id = new CacheId(INSTANCE_ID, null, TableId.of("a12"));

    Map<String,String> props = new HashMap<>();
    props.put("invalid", "aValue");

    store.setProperties(id, props);
  }

  @Test
  public void setInitialProp() throws PropCacheException {
    CacheId id = new CacheId(INSTANCE_ID, null, TableId.of("a12"));

    Map<String,String> props = new HashMap<>();
    props.put("table.bloom.enabled", "true");
    props.put("table.bulk.max.tablets", "3");

    store.setProperties(id, props);

    // direct read - confirm stored in zooKeeper
    PropEncoding readProps2 = store.readFromStore(id);

    assertNotNull(readProps2);
    assertEquals("true", readProps2.getProperty("table.bloom.enabled"));
    assertEquals("3", readProps2.getProperty("table.bulk.max.tablets"));

    log.info("read properties: {}", readProps2.getAllProperties());

    // TODO verifyById watcher?
  }

  @Test
  public void updateProp() throws PropCacheException {
    CacheId id = new CacheId(INSTANCE_ID, null, TableId.of("a12"));

    Map<String,String> props = new HashMap<>();
    props.put("table.bulk.max.tablets", "3");

    store.setProperties(id, props);

    // direct read - confirm stored in zooKeeper
    PropEncoding readProps2 = store.readFromStore(id);

    assertNotNull(readProps2);
    assertNull(readProps2.getProperty("table.bloom.enabled"));
    assertEquals("3", readProps2.getProperty("table.bulk.max.tablets"));

    log.info("read properties on store. version: {} - {}", readProps2.getDataVersion(),
        readProps2.getAllProperties());

    props.clear();

    props.put("table.bulk.max.tablets", "1");
    props.put("table.bloom.enabled", "true");

    store.setProperties(id, props);

    // direct read - confirm stored in zooKeeper
    readProps2 = store.readFromStore(id);

    assertNotNull(readProps2);
    assertEquals("true", readProps2.getProperty("table.bloom.enabled"));
    assertEquals("1", readProps2.getProperty("table.bulk.max.tablets"));

    log.info("read properties on update. version: {} - {}", readProps2.getDataVersion(),
        readProps2.getAllProperties());

    store.removeProperties(id, Arrays.asList("table.bloom.enabled"));

    readProps2 = store.readFromStore(id);

    assertNotNull(readProps2);
    assertNull(readProps2.getProperty("table.bloom.enabled"));
    assertEquals("1", readProps2.getProperty("table.bulk.max.tablets"));

    log.info("read properties on update. version: {} - {}", readProps2.getDataVersion(),
        readProps2.getAllProperties());

    // TODO verifyById watcher?
  }

  private static class MyPropWatchListener implements PropWatcher {

    private final Map<String,Integer> changeCounts = new HashMap<>();

    public MyPropWatchListener() {}

    @Override
    public void changeEvent(CacheId id) {
      log.debug("Thread: {} Changed: {}", Thread.currentThread().getName(), id);
      changeCounts.merge(id.nodeName(), 1, Integer::sum);
    }

    @Override
    public void deleteEvent(CacheId id) {
      throw new UnsupportedOperationException("Delete id: " + id);
    }

    public int getCounts(final CacheId id) {
      Integer v = changeCounts.get(id.nodeName());

      return Objects.isNull(v) ? 0 : v;

    }

    @Override
    public String toString() {
      return "MyPropWatchListener{" + "changeCounts=" + changeCounts + '}';
    }
  }

  @Test
  public void x() {
    IntStream.range(1, 2).forEach(n -> log.info("N: {}", n));
  }
}
