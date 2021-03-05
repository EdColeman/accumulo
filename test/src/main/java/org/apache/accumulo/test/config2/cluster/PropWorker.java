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
package org.apache.accumulo.test.config2.cluster;

import java.util.Map;

import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.PropCacheException;
import org.apache.accumulo.server.conf2.impl.ZooPropStore;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parent class for test workers that interact with a ZooPropStore
 */
public abstract class PropWorker extends TestWorker {

  public static final int WORKER_DELAY = 5000;

  private static final Logger log = LoggerFactory.getLogger(PropWorker.class);

  private final ZooKeeper zoo;
  private final ZooPropStore store;

  private final ExpectedValues truth;

  public PropWorker(final String zkConnString, final String workerId, final String instanceId,
      final ExpectedValues truth) throws Exception {
    super(zkConnString, workerId);

    this.truth = truth;

    this.zoo = new ZooKeeper(zkConnString, 5_000, null);
    ZooKeeperTestingServer.addDefaultAuth(zoo);

    store = new ZooPropStore.Builder().withZk(zoo).forInstance(instanceId).build();

  }

  public ExpectedValues getExpectedValues() {
    return truth;
  }

  public ExpectedProps getExpected(final CacheId id) {
    return truth.readExpected(id);
  }

  public void updateExpected(final CacheId id) {
    truth.updateStore(store, id);
  }

  public CacheId pickRandomTable() {
    return truth.pickRandomTable();
  }

  public Map<String,String> readPropsFromStore(final CacheId id) {
    return store.readFromStore(id).getAllProperties();
  }

  public void writePropsToStore(final CacheId id, final Map<String,String> props)
      throws PropCacheException {
    store.setProperties(id, props);
  }
}
