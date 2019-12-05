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
package org.apache.accumulo.server.conf.propstore;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.apache.accumulo.server.conf.propstore.NamedPoolFactory.createNamedThreadFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf.propstore.tasks.DataChangedTask;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooBackingStore implements Watcher {

  private static final Logger log = LoggerFactory.getLogger(ZooBackingStore.class);

  private final ZooKeeper zoo;
  private final String rootPath;

  private ExecutorService pool;

  private Map<TableId,CacheStats> watchedTables = new HashMap<>();

  public ZooBackingStore(final String rootPath, final ZooKeeper zoo) {
    this.rootPath = rootPath;
    this.zoo = zoo;

    pool = newCachedThreadPool(createNamedThreadFactory(rootPath));
  }

  public ZkMap readTableProps(final TableId tableId) {
    Objects.requireNonNull(tableId, "Table id cannot be null");
    // return watchedTables.computeIfAbsent(tableId,
    // tableId1 -> ZooFunc.getFromZookeeper(rootPath, tableId1, zoo));
    return null;
  }

  public boolean storeTableProps(ZkMap props) {
    Objects.requireNonNull(props, "Invalid call to setTableProps. Table properties cannot be null");
    try {

      String path = rootPath + "/" + props.getTableId().canonical();

      Stat stat = zoo.exists(path, false);

      log.info("node stat check for {}: {}", path, ZooFunc.prettyStat(stat));

      if (stat == null) {
        // TODO - check map version
        props.setDataVersion(0);
        zoo.create(path, props.toJson(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // TODO dev only
        log.info("Created stats: {}", ZooFunc.prettyStat(zoo.exists(path, false)));

        signalSentinelUpdate(props, SentinelUpdate.Reasons.NEW_TABLE);

      } else {

        try {
          zoo.setData(path, props.toJson(), stat.getVersion());
        } catch (KeeperException.BadVersionException ex) {
          adjudicate(stat.getVersion(), props);
        }
        signalSentinelUpdate(props, SentinelUpdate.Reasons.UPDATE);

        // TODO dev only
        log.info("Update stats: {}", ZooFunc.prettyStat(zoo.exists(path, false)));

      }

    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "Could not set props in zookeeper for [" + props.toString() + "]", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    return false;
  }

  @Override
  public void process(WatchedEvent event) {
    log.trace("Received event: {}", event);
    switch (event.getType()) {
      case None:
        log.info("Received session changed event notification. State: {}", event.getState().name());
        break;
      case NodeDataChanged:
        log.debug("Received data change event: {}", event);
        try {
          Stat stat = new Stat();
          byte[] data = zoo.getData(rootPath, this, stat);
          pool.submit(new DataChangedTask(rootPath, zoo, this, data, stat));
        } catch (KeeperException ex) {
          pool.submit(new DataChangedTask(rootPath, zoo));
          throw new IllegalStateException("Exception trying to process data change event");
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
        break;
      default:
        log.info("Unhandled type: {}", event);
    }
  }

  private void adjudicate(int version, ZkMap props) {
    // TODO replace
    throw new UnsupportedOperationException("Data changed - try to merge changes or reject update");
  }

  private void signalSentinelUpdate(ZkMap props, SentinelUpdate.Reasons newTableProps)
      throws KeeperException, InterruptedException {
    SentinelUpdate reason = new SentinelUpdate(newTableProps, props.getTableId());
    zoo.setData(rootPath, reason.toJson(), -1);
  }

}
