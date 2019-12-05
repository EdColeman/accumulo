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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkMapNode {

  private enum SyncState {
    VALID, INVALID, CHANGED, DELETING
  }

  private static final Logger log = LoggerFactory.getLogger(ZkMapNode.class);

  private AtomicLong nodeVersionId = new AtomicLong();
  private AtomicReference<String> sentinelPath = new AtomicReference<>();

  private ZkMap map;

  private ZooKeeper zoo;

  public ZkMapNode(final ZooKeeper zoo, final TableId tableId) {
    this(zoo, tableId, Collections.emptyMap());
  }

  public ZkMapNode(final ZooKeeper zoo, final TableId tableId, final Map<Property,String> props) {
    this.zoo = zoo;
    map = new ZkMap(tableId, -1);
    props.forEach((key, value) -> map.set(key, value));
  }

}
