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
package org.apache.accumulo.server.conf.propstore.tasks;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf.propstore.SentinelUpdate;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataChangedTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(DataChangedTask.class);

  private final String path;
  private final ZooKeeper zoo;
  private final SentinelUpdate reason;
  private final Stat stat;
  private final Watcher watcher;

  public DataChangedTask(final String path, final ZooKeeper zoo) {
    this(path, zoo, null, null, null);
  }

  public DataChangedTask(final String path, final ZooKeeper zoo, final Watcher watcher,
      final byte[] data, final Stat stat) {
    this.path = path;
    this.zoo = zoo;
    this.watcher = watcher;

    if (data != null) {
      this.reason = SentinelUpdate.fromJson(data);
    } else {
      this.reason = new SentinelUpdate(SentinelUpdate.Reasons.UNKNOWN, null);
    }
    this.stat = stat;
  }

  @Override
  public void run() {
    Stat stat = new Stat();
    SentinelUpdate reason;
    try {
      byte[] data = zoo.getData(path, false, stat);
      if (data != null) {
        reason = SentinelUpdate.fromJson(data);
        TableId tableId = reason.getTableId();
        // ZkMap m = ZooFunc.getFromZookeeper(path, tableId, zoo);
        // TODO need to pass update to "manager" class.
        // watcher.update(tableId, m);
        log.info("process::{}", reason);
      }

    } catch (KeeperException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

  }
}
