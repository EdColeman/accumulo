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

import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertNotNull;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooPropStoreTest {

  private static final Logger log = LoggerFactory.getLogger(ZooPropStoreTest.class);
  private static transient boolean haveZookeeper = false;
  private static ZooKeeper zookeeper;
  private final String unoInstId = "2b0234bb-c157-4de5-bed0-0446528f50e8";

  @Test
  public void mockNoConnection() {

    ZooKeeper mockZk = mock(ZooKeeper.class);

    expect(mockZk.getState()).andReturn(ZooKeeper.States.NOT_CONNECTED);

    replay(mockZk);

    PropStore store = new ZooPropStore(mockZk, "1234");

    ZkNotificationManager zkWatcher =
        new ZkNotificationManager(mockZk, store, "/accumulo/1234/config2");

    assertNotNull(zkWatcher);

    verify(mockZk);

  }

  @Test
  public void mockConnected() throws Exception {

    ZooKeeper mockZk = mock(ZooKeeper.class);

    expect(mockZk.getState()).andReturn(ZooKeeper.States.CONNECTED);

    Capture<Watcher> watcher = EasyMock.newCapture();

    expect(mockZk.exists(anyString(), capture(watcher))).andReturn(new Stat());

    replay(mockZk);

    PropStore store = new ZooPropStore(mockZk, "1234");

    assertNotNull(store);

    watcher.getValue().process(new WatchedEvent(Watcher.Event.EventType.NodeDataChanged,
        Watcher.Event.KeeperState.SyncConnected, "a_string"));
    watcher.getValue().process(new WatchedEvent(Watcher.Event.EventType.NodeDeleted,
        Watcher.Event.KeeperState.SyncConnected, "a_string"));

    verify(mockZk);

  }
}
