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
package org.apache.accumulo.server.conf2;

import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.accumulo.server.conf2.impl.GuavaPropStore;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuavaPropStoreTest {

  private static final Logger log = LoggerFactory.getLogger(GuavaPropStoreTest.class);

  /**
   * Get a mock zookeeper for these tests that is connected and the config node exits.
   *
   * @return a mock ZooKeeper instance.
   * @throws KeeperException
   *           matches ZooKeeper.exits() signature
   * @throws InterruptedException
   *           matches ZooKeeper.exits() signature
   */
  private ZooKeeper connectedMock() throws KeeperException, InterruptedException {
    ZooKeeper mockZk = mock(ZooKeeper.class);
    expect(mockZk.getState()).andReturn(ZooKeeper.States.CONNECTED);
    expect(mockZk.exists(isA(String.class), anyBoolean())).andReturn(new Stat());
    expect(mockZk.exists(isA(String.class), anyObject())).andReturn(new Stat());
    return mockZk;
  }

  @Test
  public void builderTest() throws Exception {

    ZooKeeper mockZk = connectedMock();

    replay(mockZk);

    GuavaPropStore props =
        new GuavaPropStore.Builder().withZk(mockZk).forInstance("a-b-c-d").build();

    log.debug("props: {}", props);

    verify(mockZk);

  }

  @Test
  public void basicStore() throws KeeperException, InterruptedException {

    ZooKeeper mockZk = connectedMock();

    // mock that the prop node does not exist.
    expect(mockZk.exists(isA(String.class), anyBoolean())).andReturn(null);
    expect(mockZk.create(anyString(), anyObject(), EasyMock.<List<ACL>>anyObject(), anyObject()))
        .andReturn("foo");
    replay(mockZk);

    var instanceId = UUID.randomUUID().toString();

    PropStore store = new GuavaPropStore.Builder().withZk(mockZk).forInstance(instanceId).build();

    PropEncoding props = new PropEncodingV1();
    store.writeToStore(new CacheId(instanceId, null, TableId.of("a")), props);

    verify(mockZk);

  }

  /**
   * Demo
   *
   * @throws Exception
   *           any exceptions are a test failure.
   */
  @Test
  public void mockDataChange() throws Exception {

    var instanceId = UUID.randomUUID().toString();
    var dataId = new CacheId(instanceId, null, TableId.of("a"));
    var zkDataPath = "/accumulo/" + instanceId + Constants.ZENCODED_CONFIG_ROOT + "/-::a";

    PropEncoding props = new PropEncodingV1();
    props.addProperty("key_1", "value_1");

    PropEncoding readProps;

    // init events
    ZooKeeper mockZk = mock(ZooKeeper.class);
    expect(mockZk.getState()).andReturn(ZooKeeper.States.CONNECTED);
    expect(mockZk.exists(isA(String.class), anyBoolean())).andReturn(new Stat());

    Capture<Watcher> watcher = EasyMock.newCapture();
    expect(mockZk.exists(anyString(), capture(watcher))).andReturn(new Stat());

    // initial write to store events (props empty)
    expect(mockZk.exists(eq(zkDataPath), eq(false))).andReturn(null);
    expect(mockZk.create(eq(zkDataPath), anyObject(), EasyMock.<List<ACL>>anyObject(), anyObject()))
        .andReturn(dataId.nodeName());

    // read from store
    Stat stat = new Stat();
    stat.setVersion(props.getDataVersion());
    expect(mockZk.exists(eq(zkDataPath), eq(false))).andReturn(stat);
    expect(mockZk.getData(eq(zkDataPath), anyObject(), anyObject())).andReturn(props.toBytes());

    // update write to store
    expect(mockZk.exists(eq(zkDataPath), eq(false))).andReturn(stat).anyTimes();
    mockZk.setData(eq(zkDataPath), anyObject(), anyInt());
    expectLastCall().andAnswer(() -> {
      log.debug("An answer");
      return null;
    }).anyTimes();

    replay(mockZk);

    PropStore store = new GuavaPropStore.Builder().withZk(mockZk).forInstance(instanceId).build();
    assertNotNull(store);

    log.info("DID: {}", dataId);

    store.writeToStore(dataId, props);

    // read - should set a watcher.
    readProps = store.readFromStore(dataId);
    log.debug("Read: {}", readProps.print(true));

    readProps.addProperty("key_2", "value_2");
    store.writeToStore(dataId, readProps);

    // verify the write does not set a watcher.
    // this is forcing a notification and does not represent a zookeeper watcher would have fired.
    // watcher.getValue().process(new WatchedEvent(Watcher.Event.EventType.NodeDataChanged,
    // Watcher.Event.KeeperState.SyncConnected, "a::b::c"));

    // watcher.getValue().process(new WatchedEvent(Watcher.Event.EventType.NodeDeleted,
    // Watcher.Event.KeeperState.SyncConnected, "a_string"));

    verify(mockZk);

  }
}
