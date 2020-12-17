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

import java.beans.PropertyChangeListener;
import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkNotificationManager implements NotificationManager, Watcher {

  private static final Logger log = LoggerFactory.getLogger(ZkNotificationManager.class);

  private final Map<String,WatchedNode> watchedNodes = new HashMap<>();

  private final ZooKeeper zookeeper;

  public ZkNotificationManager(final ZooKeeper zookeeper) {
    this.zookeeper = zookeeper;
  }

  @Override
  public PropEncoding get(final String name, final PropertyChangeListener listener) {
    return null;
  }

  @Override
  public void set(final String name, final PropEncoding props) {

  }

  @Override
  public void process(WatchedEvent watchedEvent) {

    log.debug("received zookeeper event: {}", watchedEvent);

    switch (watchedEvent.getType()) {
      case None:
        log.info("Node event NONE");
        log.info("State is {}", watchedEvent.getState());
        break;
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
      default:
        log.info("Node event UNKNOWN");
        log.info("State is {}", watchedEvent.getState());
        handleState(watchedEvent.getState());
        break;
    }
  }

  private void handleState(Event.KeeperState state) {
    switch (state) {
      case Closed:
      case Expired:
      case AuthFailed:
      case Disconnected:
      case SyncConnected:
      case ConnectedReadOnly:
      case SaslAuthenticated:
        log.info("State change: {}", state);
        break;
      default:
        log.info("Unhandled State change: {}", state);
        break;
    }
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
