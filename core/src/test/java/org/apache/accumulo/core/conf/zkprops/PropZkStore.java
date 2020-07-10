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
package org.apache.accumulo.core.conf.zkprops;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a PropStore that uses ZooKeeper.
 */
public class PropZkStore implements PropStore {

  private static final Logger log = LoggerFactory.getLogger(PropZkStore.class);

  private final ZooKeeper zkClient;

  private Map<String,PropData> cache = new HashMap<>();

  // fake transaction id;
  private int txid = 1;

  public PropZkStore(final ZooKeeper zkClient) {
    this.zkClient = zkClient;
  }

  @Override
  public PropData get(String path) {
    return null;
  }

  @Override
  public void store(PropData node) {}

  @Override
  public void setProperty(PropId.Scope scope, String path, String propName, String value) {

    try {

      // if node in local cache, use node id
      PropData node = cache.computeIfAbsent(path, n -> lookup(path, true));

      node.setProperty(propName, value);

      save(node);

    } catch (IllegalStateException ex) {
      throw new IllegalStateException("Received interrupt trying to set path " + path, ex);
    }

    // use node id
    // node.setProp(propName, value);

    // catch and handle KeeperException.BadVersion

    // ? if same, update anyway?

  }

  /**
   * Stub zookeeper functionality. With Zookeeper this would check if the node exists and either
   * retrieve it or create a new node.
   *
   * @param path
   *          path in zookeeper
   * @return the properties stored in zookeeper.
   * @Param create if true - create the node if missing, otherwise return null.
   */
  private PropData lookup(final String path, final boolean create) {

    try {

      Stat stat = zkClient.exists(path, false);
      if (Objects.nonNull(stat)) {
        byte[] data = zkClient.getData(path, false, stat);
        log.debug("d:", data.length);
      } else {
        if (create) {
          stat = new Stat();
          zkClient.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, stat);
          PropData data = new PropData(path, stat.getVersion());
          log.debug("D:{}", data);
        } else {
          return null;
        }
      }

    } catch (KeeperException ex) {
      throw new IllegalStateException("Could not lookup node for path " + path, ex);
    } catch (InterruptedException ex) {
      // propagate the interrupt
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted accessing zookeeper for path " + path, ex);
    }

    return createTestNode(path);
  }

  private void save(PropData node) {
    node.updateVersion(node.getVersion() + 1);
  }

  private PropData createTestNode(final String path) {
    return new PropData(path, 1);
  }

  private int getTxId() {
    return ++txid;
  }
}
