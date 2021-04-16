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

import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.zookeeper.ZooKeeper;
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkPropCacheITTest {

  private static final Logger log = LoggerFactory.getLogger(ZkPropCacheITTest.class);

  @Test
  public void readTest() {

    ZooKeeper zooKeeper = EasyMock.mock(ZooKeeper.class);
    var iid = UUID.randomUUID().toString();
    var cacheId = CacheId.forTable(iid, TableId.of("a"));

    ZkPropCache cache = new ZkPropCache(zooKeeper, iid);

    log.info("cache is {}", cache);

    var props = cache.getProperties(cacheId);

    assertTrue(props.isEmpty());
  }
}
