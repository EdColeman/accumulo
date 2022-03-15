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

import static org.apache.accumulo.server.MockServerContext.getMockContextWithPropStore;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheId;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NamespaceConfigurationTest {
  private static final NamespaceId NSID = NamespaceId.of("namespace");

  private InstanceId iid;
  private ServerContext context;
  private AccumuloConfiguration parent;
  private NamespaceConfiguration nsConfig;

  private PropStore propStore;

  @BeforeEach
  public void setUp() {
    iid = InstanceId.of(UUID.randomUUID());

    propStore = createMock(ZooPropStore.class);

    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();

    ZooReaderWriter zrw = createMock(ZooReaderWriter.class);

    context = getMockContextWithPropStore(iid, zrw, propStore);

    parent = createMock(AccumuloConfiguration.class);
    reset(propStore);

    var nsCacheId = PropCacheId.forNamespace(iid, NSID);
    expect(propStore.get(eq(nsCacheId))).andReturn(new VersionedProperties(123, Instant.now(),
        Map.of(Property.INSTANCE_SECRET.getKey(), "sekrit"))).anyTimes();
    expect(propStore.getNodeVersion(eq(nsCacheId))).andReturn(123).anyTimes();
    propStore.registerAsListener(eq(nsCacheId), anyObject());
    expectLastCall().anyTimes();

    replay(propStore, context);

    nsConfig = new NamespaceConfiguration(NSID, context, parent);
  }

  @Test
  public void testGetters() {
    NamespaceConfiguration nsConfig = new NamespaceConfiguration(NSID, context, parent);
    assertEquals(NSID, nsConfig.getNamespaceId());
    assertEquals(parent, nsConfig.getParent());
  }

  @Test
  public void testGet_InZK() {
    nsConfig = new NamespaceConfiguration(NSID, context, parent);

    assertEquals("sekrit", nsConfig.get(Property.INSTANCE_SECRET));

    verify(propStore);
  }

  @Test
  public void testGet_InParent() {
    Property p = Property.INSTANCE_SECRET;

    expect(parent.get(p.getKey())).andReturn("sekrit");
    replay(parent);

    nsConfig = new NamespaceConfiguration(NSID, context, parent);

    assertEquals("sekrit", nsConfig.get(Property.INSTANCE_SECRET));

    // TODO Need to check parent and accessor usage
  }

  @Test
  public void testGet_SkipParentIfAccumuloNS() {
    reset(propStore);
    var nsId = PropCacheId.forNamespace(iid, Namespace.ACCUMULO.id());
    expect(propStore.get(eq(nsId))).andReturn(new VersionedProperties(Map.of("a", "b"))).anyTimes();
    expect(propStore.getNodeVersion(eq(nsId))).andReturn(0).anyTimes();
    propStore.registerAsListener(eq(nsId), anyObject());
    expectLastCall().anyTimes();
    replay(propStore);

    NamespaceConfiguration accumuloNsConfig =
        new NamespaceConfiguration(Namespace.ACCUMULO.id(), context, parent);
    assertNull(accumuloNsConfig.get(Property.INSTANCE_SECRET));
  }

  @Test
  public void testGetProperties() {
    Predicate<String> all = x -> true;
    Map<String,String> props = new java.util.HashMap<>();
    props.put("dog", "bark");
    props.put("cat", "meow");
    parent.getProperties(props, all);

    replay(parent);
    reset(propStore);

    var nsCacheId = PropCacheId.forNamespace(iid, NSID);
    expect(propStore.get(eq(nsCacheId)))
        .andReturn(
            new VersionedProperties(123, Instant.now(), Map.of("foo", "bar", "tick", "tock")))
        .anyTimes();
    expect(propStore.getNodeVersion(eq(nsCacheId))).andReturn(123).anyTimes();
    propStore.registerAsListener(eq(nsCacheId), anyObject());
    expectLastCall().anyTimes();

    replay(propStore);

    nsConfig = new NamespaceConfiguration(NSID, context, parent);

    nsConfig.getProperties(props, all);
    assertEquals(4, props.size());
    assertEquals("bar", props.get("foo"));
    assertEquals("tock", props.get("tick"));
    assertEquals("bark", props.get("dog"));
    assertEquals("meow", props.get("cat"));
  }

  @Test
  public void testInvalidateCache() {
    nsConfig = new NamespaceConfiguration(NSID, context, parent);

    var value = nsConfig.get(Property.INSTANCE_SECRET);
    assertEquals("sekrit", value);

    nsConfig.invalidateCache();
    verify(propStore);
  }
}
