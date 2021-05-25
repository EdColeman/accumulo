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
package org.apache.accumulo.server.confRewrite.impl;

import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.confRewrite.zk.ZkProperties;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class PropStore implements ZkProperties {

  private final ZooKeeper zooKeeper;

  public PropStore(final ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
  }

  @Override
  public PropEncoding readFromStore(final CacheId id) {
    return null;
  }

  @Override
  public PropEncoding readFromStore(final CacheId id, Stat stat) {
    return null;
  }

  @Override
  public Stat readZkStat(final CacheId id) {
    try {
      return zooKeeper.exists(id.path(), false);
    } catch (KeeperException ex) {
      throw new IllegalStateException("Could not read node stat for " + id, ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted reading stat from zookeeper " + id, ex);
    }
  }

  @Override
  public void writeToStore(final CacheId id, final PropEncoding props) {

  }

  @Override
  public void deleteFromStore(final CacheId id) {

  }
}
