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

import java.util.Optional;

import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.PropCache;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkLoaderImpl implements ZkDataEventHandler {

  private static final Logger log = LoggerFactory.getLogger(ZkLoaderImpl.class);

  private final ZooKeeper zooKeeper;
  private final PropCache cache;

  private ZkEventProcessor eventProcessor;

  public ZkLoaderImpl(final ZooKeeper zooKeeper, final PropCache cache) {
    this.zooKeeper = zooKeeper;
    this.cache = cache;

    eventProcessor = new ZkEventProcessor(this);

  }

  public Optional<PropEncoding> readFromZooKeeper(CacheId id) {
    Stat stat = new Stat();
    try {
      byte[] data = zooKeeper.getData(id.path(), eventProcessor, stat);
      log.info("Stat: {}", stat);
      return Optional.of(new PropEncodingV1(data));
    } catch (KeeperException.NoNodeException ex) {
      return Optional.empty();
    } catch (KeeperException ex) {
      throw new IllegalStateException("Exception reading " + id + " from ZooKeeper", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted reading " + id + " from ZooKeeper", ex);
    }
  }

  @Override
  public void invalidateData() {

  }

  @Override
  public void processDelete(String zkPath) {

  }

  @Override
  public void processDataChange(String zkPath) {

  }
}
