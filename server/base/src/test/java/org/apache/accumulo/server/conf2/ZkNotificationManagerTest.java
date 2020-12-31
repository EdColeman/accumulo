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
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.accumulo.server.conf2.impl.ZooPropStore;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkNotificationManagerTest {

  private static final Logger log = LoggerFactory.getLogger(ZkNotificationManagerTest.class);

  /**
   * This test should call zookeeper getState() once - when session is closed we should stop - a new
   * Zookeeper client needs to be created.
   */
  @Test
  public void sessionClosedTest() {

    ZooKeeper mockZk = mock(ZooKeeper.class);
    expect(mockZk.getState()).andReturn(ZooKeeper.States.CLOSED);
    replay(mockZk);

    try {

      new ZooPropStore.Builder().withZk(mockZk).forInstance("a-b-c-d").build();

      fail("Expected exception with no zookeeper connection");
    } catch (IllegalStateException ex) {
      log.trace("Expected IllegalStateException thrown", ex);
      // empty.
    }

    verify(mockZk);

  }

  /**
   * This test should call zookeeper getState()multiple times, but then throw an exception when the
   * connection is not established after the retries are exhausted.
   */
  @Test
  public void sessionNotConnected() {

    ZooKeeper mockZk = mock(ZooKeeper.class);

    expect(mockZk.getState()).andReturn(ZooKeeper.States.NOT_CONNECTED).anyTimes();

    replay(mockZk);

    try {

      ZooPropStore store = new ZooPropStore.Builder().withZk(mockZk).forInstance("a-b-c-d").build();

      log.debug("s: {}", store);

      fail("Expected exception with no zookeeper connection");
    } catch (IllegalStateException ex) {
      // expected.
    }
    verify(mockZk);

  }

  /**
   * Go path - session connected and should call zookeeper exists to confirm parent configuration
   * node exists.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void sessionConnectedTest() throws Exception {

    ZooKeeper mockZk = mock(ZooKeeper.class);

    expect(mockZk.getState()).andReturn(ZooKeeper.States.CONNECTED);
    expect(mockZk.exists(isA(String.class), anyBoolean())).andReturn(new Stat());

    expect(mockZk.exists(isA(String.class), isA(Watcher.class))).andReturn(new Stat());

    replay(mockZk);

    ZooPropStore store = new ZooPropStore.Builder().withZk(mockZk).forInstance("a-b-c-d").build();

    assertTrue(store.isReady());

    verify(mockZk);

  }

  /**
   * This test tests that we can transition from connecting to connected.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void sessionConnectingTest() throws Exception {

    ZooKeeper mockZk = mock(ZooKeeper.class);

    expect(mockZk.getState()).andReturn(ZooKeeper.States.CONNECTING);
    expect(mockZk.getState()).andReturn(ZooKeeper.States.CONNECTED);
    expect(mockZk.exists(isA(String.class), anyBoolean())).andReturn(new Stat());

    expect(mockZk.exists(isA(String.class), isA(Watcher.class))).andReturn(new Stat());

    replay(mockZk);

    ZooPropStore store = new ZooPropStore.Builder().withZk(mockZk).forInstance("a-b-c-d").build();

    assertTrue(store.isReady());

    verify(mockZk);

  }

  /**
   * Change events tests.
   */
  @Test
  public void getDataTest() throws Exception {

    ZooKeeper mockZk = mock(ZooKeeper.class);

    expect(mockZk.getState()).andReturn(ZooKeeper.States.CONNECTED);
    expect(mockZk.exists(isA(String.class), anyBoolean())).andReturn(new Stat());

    Capture<Watcher> watcher = EasyMock.newCapture();

    expect(mockZk.exists(anyString(), capture(watcher))).andReturn(new Stat());

    replay(mockZk);

    ZooPropStore store = new ZooPropStore.Builder().withZk(mockZk).forInstance("a-b-c-d").build();

    assertNotNull(store);

    var path = "/accumulo/inst_id/config2/inst_id::ns1::tid1";

    watcher.getValue().process(new WatchedEvent(Watcher.Event.EventType.NodeDataChanged,
        Watcher.Event.KeeperState.SyncConnected, path));
    watcher.getValue().process(new WatchedEvent(Watcher.Event.EventType.NodeDeleted,
        Watcher.Event.KeeperState.SyncConnected, path));

    verify(mockZk);

  }

  public void watcherSnapshot() {
    // Received session event WatchedEvent state:SyncConnected type:NodeDataChanged
    // path:/dummy/dummy-1234-p
    // Received session event WatchedEvent state:Disconnected type:None path:null
    // Received session event WatchedEvent state:SyncConnected type:None path:null
    // Received session event WatchedEvent state:SyncConnected type:NodeDeleted
    // path:/dummy/dummy-1234-e0000000016
    // Received session event WatchedEvent state:Closed type:None path:null
  }
}
