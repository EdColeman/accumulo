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
package org.apache.accumulo.server.conf2.impl;

import static org.apache.zookeeper.Watcher.Event.EventType;
import static org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkConnHandler {

  public static final int READY_TIMEOUT_MILLIS = 2_000;
  private static final Logger log = LoggerFactory.getLogger(ZkConnHandler.class);

  // signal zookeeper is connected
  private final AtomicBoolean isReady = new AtomicBoolean(false);
  private final Object readyMonitor = new Object();
  private final AtomicBoolean syncRequired = new AtomicBoolean(true);

  private final ZooBackedCache cache;

  public ZkConnHandler(final ZooBackedCache cache) {
    this.cache = cache;
  }

  void disable() {
    isReady.set(false);
  }

  private void setZkConnected() {
    if (isReady.compareAndSet(false, true)) {
      synchronized (readyMonitor) {
        readyMonitor.notifyAll();
      }
    }
  }

  private void setZkDisconnected() {
    isReady.set(false);
  }

  public void blockUntilReady() throws InterruptedException {

    while (!isReady.get()) {
      syncRequired.set(true);
      synchronized (readyMonitor) {
        log.trace("Waiting for zookeeper");
        readyMonitor.wait(READY_TIMEOUT_MILLIS);
      }
    }

    if (syncRequired.compareAndSet(true, false)) {
      cache.clearAll();
    }
  }

  /**
   * Process a ZooKeeper connection event - other event types are ignored. Connection events have a
   * event type of None and set the event state. Only allow processing through the isReady flag when
   * the event state is connected. All other states should set the isReady flag to false;
   *
   * @param event
   *          a ZooKeeper watch event.
   */
  public void processWatchEvent(final WatchedEvent event) {

    if (!EventType.None.equals(event.getType())) {
      return;
    }

    if (SyncConnected.equals(event.getState())) {
      setZkConnected();
      return;
    }
    setZkDisconnected();
  }

}
