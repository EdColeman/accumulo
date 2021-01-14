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

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.fail;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkNotificationManagerTest {

  private static final Logger log = LoggerFactory.getLogger(ZkNotificationManagerTest.class);

  @Test
  public void mockNoConnection() {

    ZooKeeper mockZk = mock(ZooKeeper.class);
    PropStore store = new MemPropStore();

    expect(mockZk.getState()).andReturn(ZooKeeper.States.NOT_CONNECTED);

    replay(mockZk);

    try {
      ZkNotificationManager zkWatcher =
          new ZkNotificationManager(mockZk, store, "/accumulo/1234/config2");
      fail("Expected exception with no zookeeper connection");
    } catch (IllegalStateException ex) {
      // expected.
    }
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
