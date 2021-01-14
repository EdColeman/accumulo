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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkNotificationManager implements Watcher {

  private static final Logger log = LoggerFactory.getLogger(ZkNotificationManager.class);

  private final Map<String,WatchedNode> watchedNodes = new HashMap<>();

  private final ZooKeeper zookeeper;
  private final PropStore store;

  public ZkNotificationManager(final ZooKeeper zookeeper, final PropStore store,
      final String configRoot) {

    this.zookeeper = zookeeper;
    this.store = store;

    try {

      if (!checkConnected()) {
        throw new IllegalStateException("No connected zookeeper session");
      }

      // TODO - force the node to exist - could wait / retry or create, but assuming that's done in
      // main init()
      // also, this is creating a watcher that will not exist until the node is created.
      if (Objects.isNull(zookeeper.exists(configRoot, this))) {
        throw new IllegalStateException(
            "Root configuration path " + configRoot + " does not exists");
      }

      store.enable();

    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "Could not verify root configuration path " + configRoot + " exists", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Could not verify root configuration path " + configRoot + " exists", ex);
    }
  }

  /**
   * Use Zookeeper.getState() to verify we are connected. If not connected, but not in a closed
   * state, wait for a connection to be established (or time out). The state returned by getState()
   * is a separate enum from the Watcher.KeeperState - but both convey essentially the same info.
   *
   * @return true if connected, false if not.
   */
  private boolean checkConnected() {

    ZooKeeper.States currState = zookeeper.getState();

    switch (currState) {

      case NOT_CONNECTED:
      case CONNECTING:
      case ASSOCIATING:
        boolean haveConnection = pauseForConnection();
        if (haveConnection) {
          ready();
        }
        return haveConnection;

      case CONNECTED:
      case CONNECTEDREADONLY:
        ready();
        return true;

      case CLOSED:
      case AUTH_FAILED:
      default:
        closedConnection();
        return false;
    }

  }

  /**
   * Handle the session state events from watchers. If the state is disconnected, disable reading
   * from the cache because we cannot determine if the information is current. The zookeeper client
   * will automatically retry and a connection event received (if successful) or a disconnected /
   * closed event if not.
   *
   * @param state
   *          The zookeeper session state from the Watcher event.
   * @return true if connected.
   */
  private boolean handleSessionState(ZooKeeper.States state) {

    switch (state) {
      case CONNECTED:
      case CONNECTEDREADONLY:
        return true;
      case ASSOCIATING:
      case CONNECTING:
      case AUTH_FAILED:
      case CLOSED:
      case NOT_CONNECTED:
      default:
        return false;
    }

  }

  /**
   * Stub for delaying until connection established - should be replaced by ZooReaderWriter
   *
   * @return true if connected, false otherwise.
   */
  private boolean pauseForConnection() {
    int retryCount = 5;
    while (retryCount-- > 0)
      try {
        log.debug("waiting for zookeeper connection");
        // TODO should loop / back off and be based on zookeeper timeout value.
        Thread.sleep(5_000);
        if (zookeeper.getState().equals(ZooKeeper.States.CONNECTED)) {
          return true;
        }
        return false;
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    return true;
  }

  @Override
  public void process(WatchedEvent watchedEvent) {

    log.debug("received zookeeper event: {}", watchedEvent);

    switch (watchedEvent.getType()) {
      case NodeCreated:
        log.info("Node event NODE_CREATED");
        break;
      case NodeDeleted:
        log.info("Node event NODE_DELETED");
        break;
      case NodeDataChanged:
        log.info("Node event NODE_DATA_CHANGED");
        break;
      case DataWatchRemoved:
        log.info("Node event DATA_WATCH_REMOVED");
        break;
      case ChildWatchRemoved:
        log.info("Node event CHILD_WATCH_REMOVED");
        break;
      case NodeChildrenChanged:
        log.info("Node event NODE_CHILD_CHANGED");
        break;
      case None:
      default:
        log.debug("Node event NONE - session event, with state{}", watchedEvent.getState());
        handleSessionEvent(watchedEvent.getState());
        break;
    }
  }

  /**
   * Session events are sent to all handlers so that decisions can be made on disconnect and
   * expiration events. For the prop cache, access to the cache should be controlled so that the
   * cache will only be "readable" when zookeeper is available.
   *
   * Session events (Watcher.KeeperState) and ZooKeeper.getState() (Zookeeper.States) are seperate
   * enums - but both convey essentially the same info.
   *
   * @param state
   *          The zookeeper state from the watched event.
   */
  private void handleSessionEvent(Event.KeeperState state) {
    switch (state) {
      case Closed:
      case Expired:
      case AuthFailed:
        closedConnection();
        break;
      case Disconnected:
        pauseConnection();
        break;
      case SyncConnected:
      case ConnectedReadOnly:
        ready();
        break;
      case SaslAuthenticated:
        log.trace("SaslAuthenticated State change event received.");
        break;
      default:
        log.info("Unhandled State change: {}", state);
        break;
    }
  }

  private void closedConnection() {

  }

  private void pauseConnection() {

  }

  private void ready() {

  }

  private enum ZkEventHandler {
    INIT {
      @Override
      public ZkEventHandler handle(final WatchedEvent event) {
        return HALT;
      }
    },
    HALT {
      @Override
      public ZkEventHandler handle(final WatchedEvent event) {
        return HALT;
      }
    };

    abstract ZkEventHandler handle(final WatchedEvent event);
  }

  private enum ZkStateHandler {
    INIT {
      @Override
      public ZkStateHandler handle(final WatchedEvent event) {
        return HALT;
      }
    },
    HALT {
      @Override
      public ZkStateHandler handle(final WatchedEvent event) {
        return HALT;
      }
    };

    abstract ZkStateHandler handle(final WatchedEvent event);
  }

  private static class WatchedNode {
    private final String path;
    private final Stat stat;

    public WatchedNode(final String path, final Stat stat) {
      this.path = path;
      this.stat = stat;
    }
  }
}
