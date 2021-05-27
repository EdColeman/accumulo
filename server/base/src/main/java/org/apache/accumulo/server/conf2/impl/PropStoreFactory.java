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

import org.apache.accumulo.server.conf2.PropCache;
import org.apache.accumulo.server.conf2.PropStore;
import org.apache.zookeeper.ZooKeeper;

public class PropStoreFactory {

  private ZooKeeper zooKeeper = null;
  private String instanceId = null;
  private PropCache propCache = new PropCacheImpl();

  public PropStoreFactory() {}

  public PropStoreFactory withZk(ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
    return this;
  }

  public PropStoreFactory forInstance(final String instanceId) {
    this.instanceId = instanceId;
    return this;
  }

  public PropStore build() {
    return new ZooPropStore(instanceId, zooKeeper, propCache);
  }
}
