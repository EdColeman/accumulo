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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.data.TableId;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SentinelNode {

  private static final Logger log = LoggerFactory.getLogger(SentinelNode.class);

  private final ZooBackingStore zStore;
  private final String rootPath;

  private AtomicBoolean connected = new AtomicBoolean(false);

  private AtomicInteger dataVersion = new AtomicInteger();

  private ExecutorService pool;

  private Map<TableId,ZkMap> tableProps = new HashMap<>();

  public SentinelNode(final String path, final ZooKeeper zoo) throws InterruptedException {

    log.info("path: {}", path);

    connected.set(ZooFunc.waitForZooConnection(zoo));

    this.zStore = new ZooBackingStore(path, zoo);

    this.rootPath = path;

    pool = newCachedThreadPool(createNamedThreadFactory(path));

    try {
      Stat stat = zoo.exists(path, false);
      if (stat == null) {
        throw new IllegalArgumentException("Invalid path '" + path + "'");
      }

      // byte[] data = zoo.getData(path, this, stat);
      byte[] data = zoo.getData(path, null, stat);

      dataVersion.set(stat.getVersion());

    } catch (KeeperException ex) {
      throw new IllegalStateException("invalid return from zookeeper for '" + path + "'", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  private void update(TableId tableId, ZkMap m) {
    ZkMap b = tableProps.get(tableId);

    tableProps.put(tableId, m);

    log.info("Cache update. B:{}, A:{}", b, tableProps.get(tableId));
  }

}
