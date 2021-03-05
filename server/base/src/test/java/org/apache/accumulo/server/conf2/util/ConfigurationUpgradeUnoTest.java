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
package org.apache.accumulo.server.conf2.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.accumulo.server.conf2.impl.ZooPropStore;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationUpgradeUnoTest {

  private static final Logger log = LoggerFactory.getLogger(ConfigurationUpgradeUnoTest.class);
  private static transient boolean haveZookeeper = false;
  private static ZooKeeper zookeeper;
  private static String unoInstId;

  WatchLogger watcher = new WatchLogger();

  @BeforeClass
  public static void init() {
    try {
      CountDownLatch connectionLatch = new CountDownLatch(1);
      zookeeper = new ZooKeeper("localhost:2181", 10_000, watchedEvent -> {
        if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
          connectionLatch.countDown();
        }
      });

      if (!connectionLatch.await(10, TimeUnit.SECONDS)) {
        log.info("failed tp get zookeeper connected event");
      }

      if (zookeeper.getState() == ZooKeeper.States.CONNECTED) {
        haveZookeeper = true;
      }

      byte[] bytes = zookeeper.getData("/accumulo/instances/uno", false, null);
      unoInstId = new String(bytes, UTF_8);

      var propEncodeRootPath = "/accumulo/" + unoInstId + Constants.ZENCODED_CONFIG_ROOT;
      if (zookeeper.exists(propEncodeRootPath, false) != null) {
        log.info(
            "Prop encoded configuration root {} exists, upgrade may have already run, skipping tests.",
            propEncodeRootPath);
        haveZookeeper = false;
        return;
      }
      zookeeper.addAuthInfo("digest", ("accumulo:uno").getBytes(UTF_8));

    } catch (IOException | InterruptedException | KeeperException ex) {
      log.info("Failed to connect to zookeeper - these tests will be skipped.", ex);
      haveZookeeper = false;
    }
  }

  @AfterClass
  public static void close() {
    if (haveZookeeper) {
      try {
        zookeeper.close();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Test
  public void simpleStore() {

    Assume.assumeTrue("Could not connect to zookeeper, skipping", haveZookeeper);

    // zookeeper.exists("", false);

  }

  @Test
  public void upgradeSingleTable() throws Exception {

    Assume.assumeTrue("Could not connect to zookeeper, skipping", haveZookeeper);

    // final String srcPath, final CacheId destId
    ZooPropStore propStore =
        new ZooPropStore.Builder().withZk(zookeeper).forInstance(unoInstId).build();

    ServerContext mockContext = mock(ServerContext.class);
    AccumuloConfiguration mockConf = mock(AccumuloConfiguration.class);

    expect(mockContext.getInstanceID()).andReturn(unoInstId).anyTimes();
    expect(mockContext.getZooKeepers()).andReturn("localhost:2181").anyTimes();
    expect(mockContext.getConfiguration()).andReturn(mockConf).anyTimes();
    expect(mockConf.get((Property) anyObject())).andReturn(new String("uno")).anyTimes();

    replay(mockContext, mockConf);

    ConfigurationUpgrade upgrade = new ConfigurationUpgrade(mockContext);

    CacheId id = new CacheId(unoInstId, null, TableId.of("2"));
    upgrade.upgrade("/accumulo/" + unoInstId + "/tables/2/conf", id);

    String destPath = String.format("/accumulo/%s%s/s%", unoInstId, Constants.ZENCODED_CONFIG_ROOT,
        id.nodeName());

    // read
    byte[] r = zookeeper.getData(destPath, false, null);

    PropEncoding props2 = new PropEncodingV1(r);
    log.info("Props2: {}", props2.print(true));

    upgrade.downgrade(id, "/accumulo/" + unoInstId + "/tables/2/conf");

  }

  @Test
  public void upgradeAll() throws Exception {

    Assume.assumeTrue("Could not connect to zookeeper, skipping", haveZookeeper);

    // final String srcPath, final CacheId destId
    ZooPropStore propStore =
        new ZooPropStore.Builder().withZk(zookeeper).forInstance(unoInstId).build();

    ServerContext mockContext = mock(ServerContext.class);
    AccumuloConfiguration mockConf = mock(AccumuloConfiguration.class);

    expect(mockContext.getInstanceID()).andReturn(unoInstId).anyTimes();
    expect(mockContext.getZooKeepers()).andReturn("localhost:2181").anyTimes();
    expect(mockContext.getConfiguration()).andReturn(mockConf).anyTimes();
    expect(mockConf.get((Property) anyObject())).andReturn(new String("uno")).anyTimes();

    replay(mockContext, mockConf);

    ConfigurationUpgrade upgrade = new ConfigurationUpgrade(mockContext);

    assertTrue(upgrade.performUpgrade());

    CacheId id = new CacheId(unoInstId, null, TableId.of("2"));
    upgrade.upgrade("/accumulo/" + unoInstId + "/tables/2/conf", id);

    String destPath = String.format("/accumulo/%s" + Constants.ZENCODED_CONFIG_ROOT + "/%s",
        unoInstId, id.nodeName());

    // read
    byte[] r = zookeeper.getData(destPath, false, null);

    PropEncoding props2 = new PropEncodingV1(r);
    log.info("Props2: {}", props2.print(true));

    // upgrade.downgrade(id, "/accumulo/" + unoInstId + "/tables/2/conf");

  }

  private static class WatchLogger implements Watcher {

    private static final Logger log = LoggerFactory.getLogger(WatchLogger.class);

    @Override
    public void process(WatchedEvent watchedEvent) {
      log.warn("** Received session event {}", watchedEvent);
    }
  }
}
