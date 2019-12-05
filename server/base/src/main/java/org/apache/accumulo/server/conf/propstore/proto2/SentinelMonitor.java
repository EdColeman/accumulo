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
package org.apache.accumulo.server.conf.propstore.proto2;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf.propstore.dummy.NodeData;
import org.apache.accumulo.server.conf.propstore.dummy.ZkOps;
import org.apache.accumulo.server.conf.propstore.proto2.cache.PropCache;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SentinelMonitor implements Watcher, TableIdChangeListener {

  private static final Logger log = LoggerFactory.getLogger(SentinelMonitor.class);

  private final String nodePath;
  private final PropCache cache;
  private final ZkOps zoo;

  private final AtomicReference<Stat> statRef = new AtomicReference<>();

  ExecutorService pool = Executors.newFixedThreadPool(1);

  public SentinelMonitor(final String nodePath, final PropCache cache, final ZkOps zoo) {
    this.nodePath = nodePath;
    this.cache = cache;
    this.zoo = zoo;

    Stat stat = zoo.getNodeStat(nodePath, this);
    if (stat != null) {
      statRef.set(stat);
    } else {
      throw new IllegalStateException("Could not set watcher on path '" + nodePath + '\'');
    }
  }

  @Override
  public void tableIdAdded(TableId tableId) {
    log.info("TableId added: {}", tableId);
  }

  @Override
  public void tableIdRemoved(TableId tableId) {
    log.info("TableId removed: {}", tableId);
  }

  @Override
  public void syncAll() {

  }

  @Override
  public void process(WatchedEvent event) {
    log.debug("node event: {}", event);
    if (event.getPath().compareTo(nodePath) != 0) {
      log.info("Received zookeeper event for unexpected node: '" + event.getPath() + '\'');
      return;
    }

    switch (event.getType()) {
      case NodeCreated:
        log.info("Node created event: {}", event);
        Stat stat = zoo.getNodeStat(nodePath, this);
        statRef.set(stat);
        break;
      case NodeDataChanged:
        pool.submit(new ProcessNodeDataChange(this));
        break;
      default:
        log.debug("Unhandled event");
        break;
    }
  }

  private static class ProcessNodeDataChange implements Runnable {

    private final SentinelMonitor monitor;

    public ProcessNodeDataChange(final SentinelMonitor monitor) {
      this.monitor = monitor;
    }

    @Override
    public void run() {

      NodeData nodeData = monitor.zoo.readNodeData(monitor.nodePath, monitor);

      Stat prevStat = monitor.statRef.get();
      Stat currStat = nodeData.getStat();

      // if (prevStat == null) {
      // monitor.statRef.set(currStat);
      // }

      if (prevStat.getVersion() + 1 == currStat.getVersion()) {
        log.info("Handle 1-up change");
      }

      if (prevStat.getVersion() < currStat.getVersion()) {
        log.info("Old change?");
      }

    }
  }
}
