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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import java.util.UUID;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.accumulo.server.confRewrite.PropCache;
import org.apache.accumulo.server.confRewrite.zk.ZkPropStore;
import org.easymock.EasyMock;
import org.junit.Test;

public class PropGuavaCacheTest {

  @Test
  public void loadTest() {
    var instanceId = UUID.randomUUID().toString();
    CacheId tid = CacheId.forTable(instanceId, TableId.of("a"));

    PropEncoding props = new PropEncodingV1();
    props.addProperty("key_1", "value_1");

    ZkPropStore zooProps = EasyMock.mock(ZkPropStore.class);
    expect(zooProps.readFromStore(anyObject())).andReturn(props);
    EasyMock.replay(zooProps);

    PropCache cache = new PropGuavaCache(zooProps);
    Optional<PropEncoding> returned = cache.getProperties(tid);

    assertTrue(returned.isPresent());

    EasyMock.verify(zooProps);
  }

  @Test
  public void loadMissTest() {

    var instanceId = UUID.randomUUID().toString();
    CacheId tid = CacheId.forTable(instanceId, TableId.of("a"));

    ZkPropStore zooProps = EasyMock.mock(ZkPropStore.class);
    expect(zooProps.readFromStore(anyObject())).andReturn(null);
    EasyMock.replay(zooProps);

    PropCache cache = new PropGuavaCache(zooProps);
    Optional<PropEncoding> returned = cache.getProperties(tid);

    assertTrue(returned.isEmpty());

    EasyMock.verify(zooProps);
  }

  @Test
  public void clearTest() {
    ZkPropStore zooProps = EasyMock.mock(ZkPropStore.class);
    EasyMock.replay(zooProps);

    EasyMock.verify(zooProps);
  }

  @Test
  public void clearAllTest() {
    ZkPropStore zooProps = EasyMock.mock(ZkPropStore.class);
    EasyMock.replay(zooProps);

    EasyMock.verify(zooProps);
  }

  // TODO exception testing??
}
