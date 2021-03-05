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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.PropCache;
import org.apache.zookeeper.CreateMode;
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

  private final ZooKeeper zooKeeper;
  private final PropCache cache;

  private final AtomicBoolean isReady = new AtomicBoolean(false);

  // TODO simulate upgrade occurred.
  private static final boolean ALLOW_INIT = Boolean.TRUE;

  public ZkNotificationManager(final String zkConfigRoot, final ZooKeeper zooKeeper,
      final PropCache cache) {

    Objects.requireNonNull(zooKeeper, "ZooKeeper must be provided");
    Objects.requireNonNull(cache, "A prop cache implementation must be provided.");

    this.zooKeeper = zooKeeper;
    this.cache = cache;

    try {

      if (!checkConnected()) {
        throw new IllegalStateException("No zookeeper connection");
      }

      if (ALLOW_INIT) {
        initConfig2(zkConfigRoot);
      }
      /*
       * Register watcher on node that should always exist after initialization so that session
       * events are received.
       */
      if (Objects.isNull(zooKeeper.exists(zkConfigRoot, this))) {
        throw new IllegalStateException(
            "Root configuration path " + zkConfigRoot + " does not exists");
      }
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "Could not verifyById root configuration path " + zkConfigRoot + " exist", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Could not verifyById root configuration path " + zkConfigRoot + " exist", ex);
    }
  }

  private void initConfig2(final String zkConfigRoot) {

    try {
      if (null == zooKeeper.exists(zkConfigRoot, false)) {
        log.warn("Creating default configuration node: {} - upgrade conversion did not run",
            zkConfigRoot);
        var name = zooKeeper.create(zkConfigRoot, null, ZooUtil.PUBLIC, CreateMode.PERSISTENT);
        log.trace("Created zooKeeper node: {}", name);
      }

    } catch (KeeperException ex) {
      throw new IllegalStateException("Failed to create config root node: " + zkConfigRoot, ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted trying to create config root node", ex);
    }
  }

  /**
   * Use Zookeeper.getState() to verifyById we are connected. If not connected, but not in a closed
   * state, wait for a connection to be established (or time out). The state returned by getState()
   * is a separate enum from the Watcher.KeeperState - but both convey essentially the same info.
   *
   * @return true if connected, false if not.
   */
  private boolean checkConnected() {

    ZooKeeper.States currState = zooKeeper.getState();

    log.info("Zk: {}, State: {}", zooKeeper, currState);

    switch (currState) {

      case NOT_CONNECTED:
      case CONNECTING:
      case ASSOCIATING:
        boolean haveConnection = pauseForConnection();
        if (haveConnection) {
          return ready();
        }
        return false;

      case CONNECTED:
      case CONNECTEDREADONLY:
        return ready();

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
  private boolean handleSessionState(final ZooKeeper.States state) {

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
        if (zooKeeper.getState().equals(ZooKeeper.States.CONNECTED)) {
          return true;
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    return false;
  }

  @Override
  public void process(WatchedEvent watchedEvent) {

    log.debug("received zookeeper event: {}", watchedEvent);

    switch (watchedEvent.getType()) {
      case NodeDeleted:
        var path = watchedEvent.getPath();
        log.info("CONFIG2: delete node notification {}", path);
        notifyListenersPropsChanges(watchedEvent, path);
        break;
      case NodeDataChanged:
      case NodeCreated:
        try {
          log.debug("Reasserting watcher");
          zooKeeper.exists(watchedEvent.getPath(), this);
          notifyListenersPropsChanges(watchedEvent, watchedEvent.getPath());
        } catch (KeeperException | InterruptedException ex) {
          log.warn("Failed to reset watcher", ex);
          // TODO - rethrow - or process interrupt?
        }
        break;
      case DataWatchRemoved:
      case ChildWatchRemoved:
      case NodeChildrenChanged:
        // do not care
        log.trace("Event type: {}", watchedEvent.getType());
        break;
      case None:
      default:
        log.debug("Node event NONE - session event, with state{}", watchedEvent.getState());
        handleSessionEvent(watchedEvent.getState());
        break;
    }
  }

  private void notifyListenersPropsChanges(WatchedEvent watchedEvent, String path) {
    Optional<CacheId> id = parseZkPath(path);
    if (id.isPresent()) {
      var theId = id.get();
      log.info("Data change: {}, clearing {}", watchedEvent.getType(), id);
      cache.clear(theId);
      // notify prop change listeners
      cache.changeEvent(theId);
    } else {
      log.trace("change notification received for non-prop node: {}", path);
    }
  }

  private Optional<CacheId> parseZkPath(final String path) {
    return CacheId.fromPath(path);
  }

  /**
   * Session events are sent to all handlers so that decisions can be made on disconnect and
   * expiration events. For the prop cache, access to the cache should be controlled so that the
   * cache will only be "readable" when zookeeper is available.
   * <p>
   * Session events (Watcher.KeeperState) and ZooKeeper.getState() (Zookeeper.States) are separate
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

  public boolean isReady() {
    return isReady.get();
  }

  private void closedConnection() {
    log.trace("Disable reading from store - zookeeper connection is closed");
    cache.clearAll();
    isReady.set(false);
  }

  private void pauseConnection() {
    log.trace("Disable reading from store - zookeeper connection is not connected");
    isReady.set(false);
  }

  private boolean ready() {
    isReady.set(true);
    return true;
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

    @Override
    public String toString() {
      return "WatchedNode{" + "path='" + path + '\'' + ", stat=" + stat + '}';
    }
  }
}
