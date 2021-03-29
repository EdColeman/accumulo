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
package org.apache.accumulo.test.config2.cluster;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.impl.ZooPropStore;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropWriterTest {

  public static final String INSTANCE_ID = UUID.randomUUID().toString();
  public static final String ZK_CONFIG_ROOT =
      "/accumulo/" + INSTANCE_ID + Constants.ZENCODED_CONFIG_ROOT;
  private static final Logger log = LoggerFactory.getLogger(PropWriterTest.class);
  private static ZooKeeperTestingServer szk = null;
  private static ZooKeeper zooKeeper;
  private static ServerContext mockContext = null;

  // per-test values.
  private ZooPropStore store;
  private ExpectedValues truth;
  private PropWriter writer;

  @BeforeClass
  public static void setupZk() {
    // using default zookeeper port - we don't have a full configuration
    szk = new ZooKeeperTestingServer();

    zooKeeper = szk.getZooKeeper();

    mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();

    replay(mockContext);
  }

  @AfterClass
  public static void shutdownZK() throws Exception {
    szk.close();
  }

  @Before
  public void setup() throws Exception {

    szk.initPaths(ZK_CONFIG_ROOT);

    store = new ZooPropStore.Builder().withZk(zooKeeper).forInstance(INSTANCE_ID).build();

    truth = new ExpectedValues(mockContext, 1);
    for (CacheId id : truth.getIds()) {
      store.create(id, Collections.emptyMap());
    }
    writer = new PropWriter(szk.getConn(), "writer-1", INSTANCE_ID, truth);
  }

  @After
  public void tearDown() {

    try {

      ZKUtil.deleteRecursive(zooKeeper, "/accumulo");

    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to clean-up test zooKeeper nodes.", ex);
    }
  }

  @Test
  public void initialWriteTest() throws Exception {

    writer.doWork();

    for (CacheId id : truth.getIds()) {

      log.debug("Initial value: {}", id);

      ExpectedProps expectedProp = truth.readExpected(id);

      log.debug("Prop: {}", expectedProp);

      Map<String,String> storedProps = store.readFromStore(id).getAllProperties();

      assertEquals("0", storedProps.get(ExpectedValues.PROP_INT_NAME));
      assertNotNull(storedProps.get(ExpectedValues.PROP_UPDATE_NAME));

    }
  }

  @Test
  public void updateTest() throws Exception {

    initialWriteTest();

    for (CacheId id : truth.getIds()) {
      writer.doWork();
      writer.doWork();

      ExpectedProps expectedProp = truth.readExpected(id);

      log.debug("Prop: {}", expectedProp);

      Map<String,String> storedProps = store.readFromStore(id).getAllProperties();

      assertEquals("2", storedProps.get(ExpectedValues.PROP_INT_NAME));
      assertNotNull(storedProps.get(ExpectedValues.PROP_UPDATE_NAME));

      log.debug("WL: {}", ExpectedValues.getTotalWrites());
      log.debug("RL: {}", ExpectedValues.getTotalReads());
      log.debug("ERR: {}", ExpectedValues.getTotalErrors());
    }
  }
}
