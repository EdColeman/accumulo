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

import static org.apache.accumulo.core.conf.Property.GC_PORT;
import static org.apache.accumulo.core.conf.Property.MANAGER_CLIENTPORT;
import static org.apache.accumulo.core.conf.Property.TSERV_CLIENTPORT;
import static org.apache.accumulo.core.conf.Property.TSERV_NATIVEMAP_ENABLED;
import static org.apache.accumulo.core.conf.Property.TSERV_SCAN_MAX_OPENFILES;
import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf2.PropCacheId1;
import org.apache.accumulo.server.conf2.PropStore;
import org.apache.accumulo.server.conf2.PropStoreException;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooPropStoreTest {

  private final static Logger log = LoggerFactory.getLogger(ZooPropStoreTest.class);

  @Test
  public void readFixed() throws InterruptedException, KeeperException {
    String instanceId = UUID.randomUUID().toString();
    ZooKeeper zooKeeper = EasyMock.mock(ZooKeeper.class);

    PropEncoding props = new PropEncodingV1();
    props.addProperty(TSERV_CLIENTPORT.getKey(), "1234");
    props.addProperty(TSERV_NATIVEMAP_ENABLED.getKey(), "false");
    props.addProperty(TSERV_SCAN_MAX_OPENFILES.getKey(), "2345");
    props.addProperty(MANAGER_CLIENTPORT.getKey(), "3456");
    props.addProperty(GC_PORT.getKey(), "4567");

    expect(zooKeeper.exists(anyObject(), anyObject())).andReturn(new Stat()).anyTimes();
    expect(zooKeeper.getData(isA(String.class), anyBoolean(), anyObject()))
        .andReturn(props.toBytes());

    replay(zooKeeper);

    PropStore propStore = new PropStoreFactory().forInstance(instanceId).withZk(zooKeeper).build();

    Map<String,String> fixed = propStore.readFixed();

    assertEquals(Property.fixedProperties.size(), fixed.size());

    log.info("V: {}", fixed);

    verify(zooKeeper);
  }

  @Test
  public void readFixedDefaults() throws InterruptedException, KeeperException {
    String instanceId = UUID.randomUUID().toString();
    ZooKeeper zooKeeper = EasyMock.mock(ZooKeeper.class);

    PropEncoding props = new PropEncodingV1();
    expect(zooKeeper.exists(anyObject(), anyObject())).andReturn(new Stat());
    expect(zooKeeper.getData(isA(String.class), anyBoolean(), anyObject()))
        .andReturn(props.toBytes());

    replay(zooKeeper);

    PropStore propStore = new PropStoreFactory().forInstance(instanceId).withZk(zooKeeper).build();

    Map<String,String> fixed = propStore.readFixed();

    assertEquals(Property.fixedProperties.size(), fixed.size());

    log.info("V: {}", fixed);

    verify(zooKeeper);
  }

  @Test
  public void getTableProps() throws InterruptedException, KeeperException, PropStoreException {
    String instanceId = UUID.randomUUID().toString();
    ZooKeeper zooKeeper = EasyMock.mock(ZooKeeper.class);

    PropEncoding props = new PropEncodingV1();

    Capture<String> path = newCapture();

    expect(zooKeeper.exists(anyObject(), anyObject())).andReturn(new Stat());
    expect(zooKeeper.getSessionTimeout()).andReturn(5000).anyTimes();
    expect(zooKeeper.getData(capture(path), anyObject(), anyObject())).andReturn(props.toBytes())
        .once();

    replay(zooKeeper);

    PropStore propStore = new PropStoreFactory().forInstance(instanceId).withZk(zooKeeper).build();

    PropCacheId1 tid = PropCacheId1.forTable(instanceId, TableId.of("table1"));

    assertNotNull(propStore.get(tid));

    // second call should return from cache - not zookeeper.
    assertNotNull(propStore.get(tid));

    verify(zooKeeper);
  }

  @Test
  public void propChangeEvent() {

  }

  @Test
  public void propDeleteEvent() {

  }

}
