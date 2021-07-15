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

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.UUID;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.server.MockServerContext;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.PropCacheId;
import org.apache.accumulo.server.conf2.PropChangeListener;
import org.apache.accumulo.server.conf2.PropStore;
import org.junit.Before;
import org.junit.Test;

public class ServerConfigurationFactory2Test {

  private String iid = "";
  private ServerContext context = null;
  private PropStore propStore;

  private static SiteConfiguration siteConfig = SiteConfiguration.auto();

  @Before
  public void setup() {
    iid = UUID.randomUUID().toString();
    context = MockServerContext.get();

    SiteConfiguration siteConfig = SiteConfiguration.auto();
    expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();
    expect(context.getInstanceID()).andReturn(iid).anyTimes();

    propStore = mock(PropStore.class);
    expect(propStore.readFixed()).andReturn(new HashMap<>()).anyTimes();

    expect(context.getPropStore()).andReturn(propStore).anyTimes();

  }

  @Test
  public void construction() {
    replay(context, propStore);
    ServerConfiguration config = new ServerConfigurationFactory(context, siteConfig);
    assertNotNull(config);
  }

  @Test
  public void getTableConfiguration() {
    expect(context.getZooKeepers()).andReturn("localhost:2121");
    expect(context.getZooKeepersSessionTimeOut()).andReturn(15_000);
    expect(context.getZooKeeperRoot()).andReturn("/accumulo/" + iid);
    replay(context, propStore);

    // todo - invalid - need zookeeper (table exists call)
    // ServerConfiguration2 config = new ServerConfigurationFactory2(context);
    // AccumuloConfiguration tableConfig = config.getTableConfiguration(TableId.of("tablea"));
    // assertNotNull(tableConfig);
  }

  @Test
  public void getNamespaceConfiguration() {}

  @Test
  public void getSystemConfiguration() {
    propStore.registerAsListener(isA(PropCacheId.class), isA(PropChangeListener.class));
    expectLastCall().anyTimes();
    replay(context, propStore);
    ServerConfiguration config = new ServerConfigurationFactory(context, siteConfig);
    AccumuloConfiguration systemConfig = config.getSystemConfiguration();
    assertNotNull(systemConfig);
  }
}
