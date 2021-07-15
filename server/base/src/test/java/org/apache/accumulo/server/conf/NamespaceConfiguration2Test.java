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

import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.PropEncoding;
import org.apache.accumulo.server.conf.codec.PropEncodingV1;
import org.apache.accumulo.server.conf2.PropCacheId;
import org.apache.accumulo.server.conf2.PropStore;
import org.apache.accumulo.server.conf2.impl.PropStoreFactory;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamespaceConfiguration2Test {

  private static final AtomicInteger idGen = new AtomicInteger(1);

  private final Logger logger = LoggerFactory.getLogger(NamespaceConfiguration2Test.class);
  private AccumuloConfiguration parent;
  private ServerContext serverContext;
  private ZooKeeper zooKeeper;
  private ZooReaderWriter zooReaderWriter;

  private NamespaceConfiguration populateMocks(final NamespaceId nsId, final PropEncoding sysProps,
      final PropEncoding nsProps) throws Exception {

    String iid = UUID.randomUUID().toString();

    PropCacheId sysPropsId = PropCacheId.forSystem(iid);
    PropCacheId nsPropId = PropCacheId.forNamespace(iid, nsId);

    zooKeeper = createMock(ZooKeeper.class);
    zooReaderWriter = createMock(ZooReaderWriter.class);
    expect(zooReaderWriter.getZooKeeper()).andReturn(zooKeeper).anyTimes();

    expect(zooReaderWriter.exists(anyObject(), anyObject())).andReturn(true).once();
    expect(zooReaderWriter.exists(eq(sysPropsId.getPath()))).andReturn(true).anyTimes();

    expect(zooKeeper.getData(eq(sysPropsId.getPath()), anyObject(), anyObject(Stat.class)))
        .andReturn(sysProps.toBytes()).anyTimes();

    expect(zooKeeper.getData(eq(nsPropId.getPath()), anyObject(), anyObject(Stat.class)))
        .andReturn(nsProps.toBytes()).anyTimes();

    expect(zooKeeper.getData(eq(sysPropsId.getPath()), anyBoolean(), anyObject()))
        .andReturn(sysProps.toBytes()).once();

    replay(zooKeeper, zooReaderWriter);

    PropStore propStore = new PropStoreFactory().withZk(zooReaderWriter).forInstance(iid).build();

    serverContext = createMock(ServerContext.class);
    ConfigurationCopy conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    conf.set(Property.INSTANCE_VOLUMES, "file:///");

    expect(serverContext.getInstanceID()).andReturn(iid).anyTimes();
    expect(serverContext.getPropStore()).andReturn(propStore).anyTimes();

    SiteConfiguration siteConf = SiteConfiguration.auto();
    expect(serverContext.getSiteConfiguration()).andReturn(siteConf).anyTimes();

    replay(serverContext);

    parent = new SystemConfiguration2(serverContext);

    return new NamespaceConfiguration(nsId, serverContext, parent);
  }

  @Test
  public void testGetters() throws Exception {

    NamespaceId nsId = NamespaceId.of("nstest" + idGen.incrementAndGet());

    PropEncoding sysProps = new PropEncodingV1();
    PropEncoding nsProps = new PropEncodingV1();

    NamespaceConfiguration nsConfig = populateMocks(nsId, sysProps, nsProps);
    logger.info("P:{}", parent);

    assertEquals(nsId, nsConfig.getNamespaceId());
    assertEquals(parent, nsConfig.getParentConfiguration());

    verify(serverContext, zooKeeper, zooReaderWriter);
  }

  @Test
  public void onlyInNamespace() {}

  @Test
  public void onlyNamespaceOverride() {}

  @Test
  public void testGet_SkipParentIfAccumuloNS() {}

  @Test
  public void getFromParentByName() throws Exception {

    NamespaceId nsId = NamespaceId.of("nstest" + idGen.incrementAndGet());

    PropEncoding sysProps = new PropEncodingV1();
    Property p = Property.INSTANCE_SECRET;
    sysProps.addProperty(p.getKey(), "sekrit");

    PropEncoding nsProps = new PropEncodingV1();
    NamespaceConfiguration nsConfig = populateMocks(nsId, sysProps, nsProps);

    String read = nsConfig.get(p.getKey());

    assertEquals("sekrit", read);

  }

  @Test
  public void getFromParentByProp() throws Exception {

    NamespaceId nsId = NamespaceId.of("nstest" + idGen.incrementAndGet());

    PropEncoding sysProps = new PropEncodingV1();
    Property p = Property.INSTANCE_SECRET;
    sysProps.addProperty(p.getKey(), "sekrit");

    PropEncoding nsProps = new PropEncodingV1();
    NamespaceConfiguration nsConfig = populateMocks(nsId, sysProps, nsProps);

    String read = nsConfig.get(p);

    assertEquals("sekrit", read);

  }
}
