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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.MockServerContext;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.PropEncoding;
import org.apache.accumulo.server.conf.codec.PropEncodingV1;
import org.apache.accumulo.server.conf2.PropCacheId;
import org.apache.accumulo.server.conf2.PropChangeListener;
import org.apache.accumulo.server.conf2.PropStore;
import org.apache.accumulo.server.conf2.PropStoreException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableConfiguration2Test {

  private final static Logger log = LoggerFactory.getLogger(TableConfiguration2Test.class);

  private static final TableId TID = TableId.of("table");
  private static final String ZOOKEEPERS = "localhost";
  private static final int ZK_SESSION_TIMEOUT = 120000;

  private ServerContext context;
  private NamespaceConfiguration parent;

  private TableConfiguration c;
  private PropStore propStore;

  @Before
  public void setUp() {

    String iid = UUID.randomUUID().toString();
    context = MockServerContext.getWithZK(iid, ZOOKEEPERS, ZK_SESSION_TIMEOUT);

    propStore = mock(PropStore.class);
    propStore.registerAsListener(isA(PropCacheId.class), isA(PropChangeListener.class));
    expectLastCall().anyTimes();

    expect(propStore.readFixed()).andReturn(getFixedProps()).anyTimes();

    expect(context.getPropStore()).andReturn(propStore).anyTimes();

    replay(context);
    replay(propStore);

    parent = createMock(NamespaceConfiguration.class);
    c = new TableConfiguration(context, TID, parent);

  }

  @Test
  public void construction() {

    c = new TableConfiguration(context, TID, parent);

    assertEquals(TID, c.getTableId());
    assertNotNull(c);
  }

  @Test
  public void testGetters() {
    assertEquals(TID, c.getTableId());
    assertEquals(parent, c.getParentConfiguration());
  }

  @Test
  public void getInTable() throws PropStoreException {

    c = new TableConfiguration(context, TID, parent);

    PropEncoding encoded = new PropEncodingV1();

    reset(propStore);

    var propValue = "sekritz";
    encoded.addProperty(Property.INSTANCE_SECRET.getKey(), propValue);

    expect(propStore.get(anyObject())).andReturn(encoded);

    replay(propStore);

    String v = c.get(Property.INSTANCE_SECRET);

    // todo - invalid need to mock system prop
    // assertEquals(propValue, v);
  }

  @Test
  public void getInParent() {

  }

  private Map<String,String> getFixedProps() {
    Map<String,String> fixed = new HashMap<>();
    Property.fixedProperties.forEach(p -> fixed.put(p.getKey(), p.getDefaultValue()));
    return fixed;
  }

}
