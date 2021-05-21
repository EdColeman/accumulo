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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

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
    expect(zooProps.readFromStore(anyObject())).andReturn(props).once();
    EasyMock.replay(zooProps);

    PropCache cache = new PropGuavaCache(zooProps);
    PropEncoding returned = cache.getProperties(tid);

    // load from zookeeper
    assertNotNull(cache.getProperties(tid));

    // get from cache - no zookeeper call
    assertNotNull(cache.getProperties(tid));

    EasyMock.verify(zooProps);
  }

  @Test
  public void loadMissTest() {

    var instanceId = UUID.randomUUID().toString();
    CacheId tid = CacheId.forTable(instanceId, TableId.of("a"));

    ZkPropStore zooProps = EasyMock.mock(ZkPropStore.class);
    expect(zooProps.readFromStore(anyObject())).andReturn(null).times(2);
    EasyMock.replay(zooProps);

    PropCache cache = new PropGuavaCache(zooProps);
    PropEncoding returned = cache.getProperties(tid);

    assertNull(returned);

    // the second call verifies that the cache did not store a null (no props) result.
    // this ensures that zookeeper is checked each call until a value is found / cached.
    returned = cache.getProperties(tid);
    assertNull(returned);

    EasyMock.verify(zooProps);
  }

  @Test
  public void clearTest() {
    var instanceId = UUID.randomUUID().toString();
    CacheId tid = CacheId.forTable(instanceId, TableId.of("a"));

    PropEncoding props = new PropEncodingV1();
    props.addProperty("key_1", "value_1");

    ZkPropStore zooProps = EasyMock.mock(ZkPropStore.class);
    expect(zooProps.readFromStore(anyObject())).andReturn(props).times(2);
    EasyMock.replay(zooProps);

    PropCache cache = new PropGuavaCache(zooProps);

    // load from zookeeper
    assertNotNull(cache.getProperties(tid));

    // get cached value
    assertNotNull(cache.getProperties(tid));

    cache.clear(tid);

    // (re)load from zookeeper
    assertNotNull(cache.getProperties(tid));

    EasyMock.verify(zooProps);
  }

  @Test
  public void clearAllTest() {
    var instanceId = UUID.randomUUID().toString();
    CacheId tid = CacheId.forTable(instanceId, TableId.of("a"));

    PropEncoding props = new PropEncodingV1();
    props.addProperty("key_1", "value_1");

    ZkPropStore zooProps = EasyMock.mock(ZkPropStore.class);
    expect(zooProps.readFromStore(anyObject())).andReturn(props).times(2);
    EasyMock.replay(zooProps);

    PropCache cache = new PropGuavaCache(zooProps);

    // load from zookeeper
    assertNotNull(cache.getProperties(tid));

    // get cached value
    assertNotNull(cache.getProperties(tid));

    cache.clearAll();

    // (re)load from zookeeper
    assertNotNull(cache.getProperties(tid));

    EasyMock.verify(zooProps);
  }

  @Test
  public void exception1Test() {

    var instanceId = UUID.randomUUID().toString();
    CacheId tid = CacheId.forTable(instanceId, TableId.of("a"));

    ZkPropStore zooProps = EasyMock.mock(ZkPropStore.class);
    expect(zooProps.readFromStore(anyObject()))
        .andThrow(new IllegalStateException("a fake exception"));
    EasyMock.replay(zooProps);

    PropCache cache = new PropGuavaCache(zooProps);
    try {
      PropEncoding returned = cache.getProperties(tid);
    } catch (Exception ex) {
      // expected.
      return;
    }
    fail("Expected an exception to be thrown");
  }

  // TODO additional exception testing??
}
