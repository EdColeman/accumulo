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
package org.apache.accumulo.server.conf;

import static org.apache.accumulo.core.conf.Property.GC_PORT;
import static org.apache.accumulo.core.conf.Property.MANAGER_MINTHREADS;
import static org.apache.accumulo.core.conf.Property.TABLE_BLOOM_ENABLED;
import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.MockServerContext;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.PropCacheId;
import org.apache.accumulo.server.conf2.PropChangeListener;
import org.apache.accumulo.server.conf2.PropStore;
import org.apache.accumulo.server.conf2.PropStoreException;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooBasedConfigurationTest {

  private final static Logger log = LoggerFactory.getLogger(ZooBasedConfigurationTest.class);

  private Map<String,String> getFixedProps() {
    Map<String,String> fixed = new HashMap<>();
    Property.fixedProperties.forEach(p -> fixed.put(p.getKey(), p.getDefaultValue()));
    return fixed;
  }

  private PropStore getMockPropStore() {

    PropStore propStore = mock(PropStore.class);

    expect(propStore.readFixed()).andReturn(getFixedProps());
    propStore.registerAsListener(isA(PropCacheId.class), isA(PropChangeListener.class));
    expectLastCall().anyTimes();

    return propStore;
  }

  private ServerContext getMockServerContext(final String instanceId, final PropStore propStore) {
    ServerContext context = MockServerContext.get();
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
    expect(context.getPropStore()).andReturn(propStore);
    return context;
  }

  @Test
  public void get() throws PropStoreException {

    String instanceId = UUID.randomUUID().toString();

    PropStore mockPropStore = getMockPropStore();
    PropEncoding encoded = new PropEncodingV1();
    encoded.addProperty(TABLE_BLOOM_ENABLED.getKey(), "true");

    PropEncoding invalid = new PropEncodingV1();
    invalid.addProperty(TABLE_BLOOM_ENABLED.getKey(), "1234");

    expect(mockPropStore.get(isA(PropCacheId.class))).andReturn(encoded);
    expect(mockPropStore.get(isA(PropCacheId.class))).andReturn(invalid);

    ServerContext mockContext = getMockServerContext(instanceId, mockPropStore);

    AccumuloConfiguration parent = mock(AccumuloConfiguration.class);
    expect(parent.get(isA(Property.class))).andReturn("false");

    EasyMock.replay(mockContext, mockPropStore, parent);

    PropCacheId propCacheId = PropCacheId.forTable(instanceId, TableId.of("a"));

    ZooBasedConfiguration configuration =
        new ZooBasedConfiguration(log, mockContext, propCacheId, parent);

    assertEquals("true", configuration.get(TABLE_BLOOM_ENABLED));

    // fixed - default
    assertEquals(GC_PORT.getDefaultValue(), configuration.get(GC_PORT));

    // invalid format
    assertEquals("false", configuration.get(TABLE_BLOOM_ENABLED));

    verify(mockContext, mockPropStore, parent);
  }

  @Test
  public void getFromParent() throws PropStoreException {

    String instanceId = UUID.randomUUID().toString();

    PropStore mockPropStore = getMockPropStore();
    expect(mockPropStore.get(isA(PropCacheId.class))).andReturn(null);

    ServerContext mockContext = getMockServerContext(instanceId, mockPropStore);

    AccumuloConfiguration parent = mock(AccumuloConfiguration.class);
    expect(parent.get(TABLE_BLOOM_ENABLED)).andReturn(TABLE_BLOOM_ENABLED.getDefaultValue());

    EasyMock.replay(mockContext, mockPropStore, parent);

    PropCacheId propCacheId = PropCacheId.forTable(instanceId, TableId.of("a"));

    ZooBasedConfiguration configuration =
        new ZooBasedConfiguration(log, mockContext, propCacheId, parent);

    String v = configuration.get(TABLE_BLOOM_ENABLED);

    assertEquals("false", v);

    verify(mockContext, mockPropStore, parent);
  }

  @Test
  public void getProperties() throws PropStoreException {

    Predicate<String> all = str -> true;

    String instanceId = UUID.randomUUID().toString();

    PropStore mockPropStore = getMockPropStore();
    PropEncoding encoded = new PropEncodingV1();
    encoded.addProperty(TABLE_BLOOM_ENABLED.getKey(), "true");

    expect(mockPropStore.get(isA(PropCacheId.class))).andReturn(encoded);

    ServerContext mockContext = getMockServerContext(instanceId, mockPropStore);

    Capture<Map<String,String>> mapCapture = newCapture();

    AccumuloConfiguration parent = mock(AccumuloConfiguration.class);
    parent.getProperties(capture(mapCapture), anyObject());
    expectLastCall().andAnswer(() -> {
      mapCapture.getValue().put(MANAGER_MINTHREADS.getKey(), "123");
      return null;
    });

    EasyMock.replay(mockContext, mockPropStore, parent);

    PropCacheId propCacheId = PropCacheId.forTable(instanceId, TableId.of("a"));

    ZooBasedConfiguration configuration =
        new ZooBasedConfiguration(log, mockContext, propCacheId, parent);

    Map<String,String> props = new HashMap<>();
    configuration.getProperties(props, all);

    assertEquals(2, props.size());
    assertTrue(props.containsKey(TABLE_BLOOM_ENABLED.getKey()));
    assertEquals("true", props.get(TABLE_BLOOM_ENABLED.getKey()));

    assertTrue(props.containsKey(MANAGER_MINTHREADS.getKey()));
    assertEquals("123", props.get(MANAGER_MINTHREADS.getKey()));

    verify(mockContext, mockPropStore, parent);
  }

  @Test
  public void getUpdateCount() throws PropStoreException {

    String instanceId = UUID.randomUUID().toString();

    PropStore mockPropStore = getMockPropStore();
    PropEncoding encoded = new PropEncodingV1();
    encoded.addProperty(TABLE_BLOOM_ENABLED.getKey(), "true");
    // simulate write to ZooKeeper
    encoded.toBytes();
    expect(mockPropStore.get(isA(PropCacheId.class))).andReturn(encoded).times(2);

    // serialization round-trip will increment dataVersion.
    PropEncoding updated = new PropEncodingV1(encoded.toBytes());
    // simulate write to ZooKeeper
    updated.toBytes();

    log.info("Data Version: {}", encoded.getDataVersion());
    log.info("Data Version: {}", updated.getDataVersion());

    expect(mockPropStore.get(isA(PropCacheId.class))).andReturn(updated).once();

    ServerContext mockContext = getMockServerContext(instanceId, mockPropStore);

    // override getUpdateCount to simulate a parent that changes the count
    AccumuloConfiguration parent = partialMockBuilder(AccumuloConfiguration.class)
        .addMockedMethod("getUpdateCount").createMock();

    mock(AccumuloConfiguration.class);
    expect(parent.getUpdateCount()).andReturn(0L);
    expect(parent.getUpdateCount()).andReturn(1L);
    expect(parent.getUpdateCount()).andReturn(1L);

    EasyMock.replay(mockContext, mockPropStore, parent);

    PropCacheId propCacheId = PropCacheId.forTable(instanceId, TableId.of("a"));

    ZooBasedConfiguration configuration =
        new ZooBasedConfiguration(log, mockContext, propCacheId, parent);

    long updateCount1 = configuration.getUpdateCount();
    long updateCount2 = configuration.getUpdateCount();

    log.info(String.format("U1: %016x, U2: %016x", updateCount1, updateCount2));

    // parent changed
    assertTrue(updateCount1 != updateCount2);

    // new props - same parent
    long updateCount3 = configuration.getUpdateCount();

    log.info(String.format("U2: %016x, U3: %016x", updateCount2, updateCount3));

    assertTrue(updateCount2 != updateCount3);
    configuration.changeEvent(propCacheId);

    assertNotEquals(updateCount2, updateCount3);

    verify(mockContext, mockPropStore, parent);
  }

  @Test
  public void isPropertySet() throws PropStoreException {

    String instanceId = UUID.randomUUID().toString();

    PropStore mockPropStore = getMockPropStore();
    PropEncoding encoded = new PropEncodingV1();

    PropEncoding encoded2 = new PropEncodingV1();
    encoded2.addProperty(TABLE_BLOOM_ENABLED.getKey(), "true");

    expect(mockPropStore.get(isA(PropCacheId.class))).andReturn(encoded).once();
    expect(mockPropStore.get(isA(PropCacheId.class))).andReturn(encoded2).once();

    ServerContext mockContext = getMockServerContext(instanceId, mockPropStore);

    AccumuloConfiguration parent = mock(AccumuloConfiguration.class);
    expect(parent.isPropertySet(anyObject(), anyBoolean())).andReturn(false);

    EasyMock.replay(mockContext, mockPropStore, parent);

    PropCacheId propCacheId = PropCacheId.forTable(instanceId, TableId.of("a"));

    ZooBasedConfiguration configuration =
        new ZooBasedConfiguration(log, mockContext, propCacheId, parent);

    // not set
    assertFalse(configuration.isPropertySet(TABLE_BLOOM_ENABLED, false));

    // fixed property
    assertTrue(configuration.isPropertySet(GC_PORT, false));

    // set in current
    assertTrue(configuration.isPropertySet(TABLE_BLOOM_ENABLED, false));

    // set in parent

    verify(mockContext, mockPropStore, parent);
  }
}
