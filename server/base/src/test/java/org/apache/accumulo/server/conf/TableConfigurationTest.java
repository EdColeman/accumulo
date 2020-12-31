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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.MemPropStore;
import org.apache.accumulo.server.conf2.PropCache;
import org.apache.accumulo.server.conf2.PropCacheImpl;
import org.apache.accumulo.server.conf2.PropEncoding;
import org.apache.accumulo.server.conf2.PropEncodingV1;
import org.junit.Before;
import org.junit.Test;

public class TableConfigurationTest {
  private static final TableId TID = TableId.of("table");

  private String iid;
  private ServerContext context;
  private NamespaceConfiguration parent;
  private TableConfiguration c;
  private MemPropStore memProps;
  private CacheId cacheId;

  @Before
  public void setUp() {
    iid = UUID.randomUUID().toString();
    cacheId = new CacheId(iid, null, TID);

    context = createMock(ServerContext.class);
    parent = createMock(NamespaceConfiguration.class);

    memProps = new MemPropStore();
    PropCache propCache = new PropCacheImpl(memProps);

    expect(context.getInstanceID()).andReturn(iid).anyTimes();
    expect(context.getConfiguration()).andReturn(parent).anyTimes();
    expect(context.getPropCache()).andReturn(propCache).anyTimes();
    replay(context);

    c = new TableConfiguration(context, TID, parent);
  }

  @Test
  public void testGetters() {
    assertEquals(TID, c.getTableId());
  }

  @Test
  public void testGet_InZK() {
    Property p = Property.INSTANCE_SECRET;
    PropEncoding props = new PropEncodingV1(1, false, Instant.now());
    props.addProperties(Map.of(p.getKey(), "sekrit"));
    memProps.set(cacheId, props);
    assertEquals("sekrit", c.get(p));
  }

  @Test
  public void testGet_InParent() {
    Property p = Property.INSTANCE_SECRET;
    expect(parent.get(p)).andReturn("sekrit");
    replay(parent);
    PropEncoding props = new PropEncodingV1(1, false, Instant.now());
    props.addProperties(Map.of());
    memProps.set(cacheId, props);
    assertEquals("sekrit", c.get(p));
  }

  @Test
  public void testGetProperties() {
    Predicate<String> all = x -> true;
    Map<String,String> propsResult = new HashMap<>();
    parent.getProperties(propsResult, all);
    expectLastCall().anyTimes();
    replay(parent);
    PropEncoding props = new PropEncodingV1(1, false, Instant.now());
    props.addProperties(Map.of("foo", "bar", "ding", "dong"));
    memProps.set(cacheId, props);
    c.getProperties(propsResult, all);
    assertEquals(2, propsResult.size());
    assertEquals("bar", propsResult.get("foo"));
    assertEquals("dong", propsResult.get("ding"));
  }

  @Test
  public void testInvalidateCache() {
    Property p = Property.INSTANCE_SECRET;
    PropEncoding props = new PropEncodingV1(1, false, Instant.now());
    props.addProperties(Map.of(p.getKey(), "sekrit"));
    memProps.set(cacheId, props);
    context.getPropCache().clearProperties(cacheId);
    assertEquals("sekrit", c.get(p));
    c.invalidateCache();
    assertEquals("sekrit", c.get(p));
  }
}
