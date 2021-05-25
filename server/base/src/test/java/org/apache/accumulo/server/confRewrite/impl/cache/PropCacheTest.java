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
package org.apache.accumulo.server.confRewrite.impl.cache;

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
import org.apache.accumulo.server.confRewrite.zk.ZkProperties;
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropCacheTest {

  private static final Logger log = LoggerFactory.getLogger(PropCacheTest.class);

  @Test
  public void loadTestGuava() {
    PropEncoding props = new PropEncodingV1();
    props.addProperty("key_1", "value_1");

    ZkProperties mockZkProperties = EasyMock.mock(ZkProperties.class);
    expect(mockZkProperties.readFromStore(anyObject())).andReturn(props).once();
    PropCache cache = new PropGuavaCache(mockZkProperties);
    loadTest(cache, mockZkProperties);
  }

  @Test
  public void loadTestPropTTL() {
    PropEncoding props = new PropEncodingV1();
    props.addProperty("key_1", "value_1");

    ZkProperties mockZkProperties = EasyMock.mock(ZkProperties.class);
    expect(mockZkProperties.readFromStore(anyObject(), anyObject())).andReturn(props).once();

    PropCache cache = new PropTTLCache(mockZkProperties);
    loadTest(cache, mockZkProperties);
  }

  private void loadTest(final PropCache cache, final ZkProperties mockZkProperties) {
    CacheId tid = generateCacheId();

    EasyMock.replay(mockZkProperties);

    // load from zookeeper
    assertNotNull(cache.getProperties(tid));

    // get from cache - no zookeeper call
    assertNotNull(cache.getProperties(tid));

    EasyMock.verify(mockZkProperties);
  }

  @Test
  public void loadMissGuavaTest() {
    PropEncoding props = new PropEncodingV1();
    props.addProperty("key_1", "value_1");

    ZkProperties mockZkProperties = EasyMock.mock(ZkProperties.class);
    expect(mockZkProperties.readFromStore(anyObject())).andReturn(null).times(2);
    PropCache cache = new PropGuavaCache(mockZkProperties);

    loadMissTest(cache, mockZkProperties);
  }

  @Test
  public void loadMissPropTTLTest() {
    PropEncoding props = new PropEncodingV1();
    props.addProperty("key_1", "value_1");

    ZkProperties mockZkProperties = EasyMock.mock(ZkProperties.class);
    expect(mockZkProperties.readFromStore(anyObject(), anyObject())).andReturn(null).times(2);
    PropCache cache = new PropTTLCache(mockZkProperties);

    loadMissTest(cache, mockZkProperties);
  }

  private void loadMissTest(final PropCache cache, final ZkProperties mockZkProperties) {

    CacheId tid = generateCacheId();

    EasyMock.replay(mockZkProperties);

    PropEncoding returned = cache.getProperties(tid);

    assertNull(returned);

    // the second call verifies that the cache did not store a null (no props) result.
    // this ensures that zookeeper is checked each call until a value is found / cached.
    returned = cache.getProperties(tid);
    assertNull(returned);

    EasyMock.verify(mockZkProperties);
  }

  private CacheId generateCacheId() {
    var instanceId = UUID.randomUUID().toString();
    return CacheId.forTable(instanceId, TableId.of("a"));
  }

  @Test
  public void clearGuavaTest() {
    PropEncoding props = new PropEncodingV1();
    props.addProperty("key_1", "value_1");

    ZkProperties mockZkProperties = EasyMock.mock(ZkProperties.class);
    PropCache cache = new PropGuavaCache(mockZkProperties);

    expect(mockZkProperties.readFromStore(anyObject())).andReturn(props).times(2);

    clearTest(cache, mockZkProperties);
  }

  @Test
  public void clearPropTTLTest() {
    PropEncoding props = new PropEncodingV1();
    props.addProperty("key_1", "value_1");

    ZkProperties mockZkProperties = EasyMock.mock(ZkProperties.class);
    PropCache cache = new PropTTLCache(mockZkProperties);

    expect(mockZkProperties.readFromStore(anyObject(), anyObject())).andReturn(props).times(2);

    clearTest(cache, mockZkProperties);
  }

  private void clearTest(final PropCache cache, final ZkProperties mockZkProperties) {
    CacheId tid = generateCacheId();

    EasyMock.replay(mockZkProperties);

    // load from zookeeper
    assertNotNull(cache.getProperties(tid));

    // get cached value
    assertNotNull(cache.getProperties(tid));

    cache.clear(tid);

    // (re)load from zookeeper
    assertNotNull(cache.getProperties(tid));

    EasyMock.verify(mockZkProperties);
  }

  @Test
  public void clearAllGuavaTest() {
    PropEncoding props = new PropEncodingV1();
    props.addProperty("key_1", "value_1");

    ZkProperties mockZkProperties = EasyMock.mock(ZkProperties.class);
    PropCache cache = new PropGuavaCache(mockZkProperties);

    expect(mockZkProperties.readFromStore(anyObject())).andReturn(props).times(2);

    clearAllTest(cache, mockZkProperties);
  }

  @Test
  public void clearAllPropTTLTest() {
    PropEncoding props = new PropEncodingV1();
    props.addProperty("key_1", "value_1");

    ZkProperties mockZkProperties = EasyMock.mock(ZkProperties.class);
    PropCache cache = new PropTTLCache(mockZkProperties);

    expect(mockZkProperties.readFromStore(anyObject(), anyObject())).andReturn(props).times(2);

    clearAllTest(cache, mockZkProperties);
  }

  private void clearAllTest(final PropCache cache, final ZkProperties mockZkProperties) {
    CacheId tid = generateCacheId();

    EasyMock.replay(mockZkProperties);

    // load from zookeeper
    assertNotNull(cache.getProperties(tid));

    // get cached value
    assertNotNull(cache.getProperties(tid));

    cache.clearAll();

    // (re)load from zookeeper
    assertNotNull(cache.getProperties(tid));

    EasyMock.verify(mockZkProperties);
  }

  @Test
  public void exceptionGuavaTest() {

    ZkProperties mockZkProperties = EasyMock.mock(ZkProperties.class);

    expect(mockZkProperties.readFromStore(anyObject()))
        .andThrow(new IllegalStateException("a fake exception"));

    PropCache cache = new PropGuavaCache(mockZkProperties);

    exception1Test(cache, mockZkProperties);
  }

  @Test
  public void exceptionCacheTTLTest() {

    ZkProperties mockZkProperties = EasyMock.mock(ZkProperties.class);

    expect(mockZkProperties.readFromStore(anyObject(), anyObject()))
        .andThrow(new IllegalStateException("a fake exception"));

    PropCache cache = new PropTTLCache(mockZkProperties);

    exception1Test(cache, mockZkProperties);
  }

  private void exception1Test(final PropCache cache, final ZkProperties mockZkProperties) {

    CacheId tid = generateCacheId();
    EasyMock.replay(mockZkProperties);

    try {
      PropEncoding returned = cache.getProperties(tid);
      log.info("Failing test expected test to throw exception - table id {}", returned);
    } catch (Exception ex) {
      // expected.
      return;
    }
    fail("Expected an exception to be thrown");
  }

  // TODO additional exception testing??
}
