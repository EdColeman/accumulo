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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a PropStore that uses ZooKeeper.
 */
public class PropZkStore implements PropStore {

  private static final Logger log = LoggerFactory.getLogger(PropZkStore.class);

  private Map<String,PropData> store = new HashMap<>();

  // fake transaction id;
  private int txid = 1;

  @Override
  public PropData get(String path) {
    return null;
  }

  @Override
  public void store(PropData node) {}

  @Override
  public void setProperty(PropId.Scope scope, String path, String propName, String value) {

    // if node in local cache, use node id
    PropData node = store.computeIfAbsent(path, n -> lookup(path));

    node.setProperty(propName, value);

    save(node);

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
   */
  private PropData lookup(final String path) {
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
