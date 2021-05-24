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
package org.apache.accumulo.server.confRewrite.zk.impl;

import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.confRewrite.zk.ZkDataEventHandler;
import org.apache.accumulo.server.confRewrite.zk.ZkPropStore;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZkPropStoreImpl implements ZkPropStore {

  private final ZooKeeper zooKeeper;
  private final ZkDataEventHandler eventHandler;

  public ZkPropStoreImpl(final ZooKeeper zooKeeper, final ZkDataEventHandler eventHandler) {
    this.zooKeeper = zooKeeper;
    this.eventHandler = eventHandler;
  }

  @Override
  public PropEncoding readFromStore(final CacheId id) {
    return readFromStore(id, new Stat());
  }

  @Override
  public PropEncoding readFromStore(final CacheId id, final Stat stat) {
    return null;
  }

  @Override
  public Stat readZkStat(final CacheId id) {
    return null;
  }

  @Override
  public void writeToStore(final CacheId id, final PropEncoding props) {

  }

  @Override
  public void deleteFromStore(final CacheId id) {

  }

}
