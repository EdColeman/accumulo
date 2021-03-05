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
package org.apache.accumulo.server.conf2.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationUpgradeTest {

  private static final Logger log = LoggerFactory.getLogger(ConfigurationUpgradeTest.class);

  @SuppressWarnings("deprecation")
  @Test
  public void sysPathTest() throws Exception {
    String instanceId = UUID.randomUUID().toString();

    ServerContext mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(instanceId).anyTimes();
    ZooKeeper mockZk = mock(ZooKeeper.class);

    replay(mockContext, mockZk);

    ConfigurationUpgrade upgrade = new ConfigurationUpgrade(mockContext, mockZk, true);

    var srcSysPath = upgrade.getSrcSysPath();
    assertEquals("/accumulo/" + instanceId + Constants.ZCONFIG, srcSysPath);
  }

  @Test
  public void nsPathTest() throws Exception {
    String instanceId = UUID.randomUUID().toString();
    NamespaceId nid = NamespaceId.of("abc");
    var base = Constants.ZROOT + "/" + instanceId;

    ServerContext mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(instanceId).anyTimes();
    ZooKeeper mockZk = mock(ZooKeeper.class);
    replay(mockContext, mockZk);

    ConfigurationUpgrade upgrade = new ConfigurationUpgrade(mockContext, mockZk, true);

    var nidPath = upgrade.getSrcNsPath(nid);

    log.info("NID: {}", nidPath);

    assertEquals("/accumulo/" + instanceId + "/namespaces/" + nid.canonical() + "/conf", nidPath);
  }

  @Test
  public void tidPathTest() throws Exception {
    String instanceId = UUID.randomUUID().toString();
    TableId tid = TableId.of("zyx");
    var base = Constants.ZROOT + "/" + instanceId;

    ServerContext mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(instanceId).anyTimes();
    ZooKeeper mockZk = mock(ZooKeeper.class);
    replay(mockContext, mockZk);

    ConfigurationUpgrade upgrade = new ConfigurationUpgrade(mockContext, mockZk, true);

    var tidPath = upgrade.getSrcTablePath(tid);

    log.info("TID: {}", tidPath);

    assertEquals("/accumulo/" + instanceId + "/tables/" + tid.canonical() + "/conf", tidPath);
  }

  @Test
  public void mockConvertSystem() throws Exception {

    String instanceId = UUID.randomUUID().toString();

    ServerContext mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(instanceId).anyTimes();

    ZooKeeper mockZk = mock(ZooKeeper.class);
    expect(mockZk.exists(anyString(), anyBoolean())).andReturn(new Stat()).anyTimes();
    expect(mockZk.getChildren(anyString(), anyBoolean())).andReturn(Stream
        .of("table.split.threshold", "tserver.walog.max.referenced").collect(Collectors.toList()));
    expect(mockZk.getData(anyString(), anyObject(), anyObject())).andReturn("512M".getBytes(UTF_8));

    expect(mockZk.getData(anyString(), anyObject(), anyObject())).andReturn("5".getBytes(UTF_8));

    expect(mockZk.setData(anyString(), anyObject(), anyInt())).andReturn(new Stat());
    replay(mockContext, mockZk);

    ConfigurationUpgrade upgrade = new ConfigurationUpgrade(mockContext, mockZk, true);

    upgrade.convertSystem();

  }

  @Test
  public void mockConvertNamespaces() throws Exception {

    String instanceId = UUID.randomUUID().toString();

    ServerContext mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(instanceId).anyTimes();
    ZooKeeper mockZk = mock(ZooKeeper.class);
    expect(mockZk.getChildren(anyString(), anyBoolean()))
        .andReturn(Stream.of("+accumulo").collect(Collectors.toList()));

    // TODO - more realistic example with multiple

    // expect(mockZk.getChildren(anyString(), anyBoolean()))
    // .andReturn(Stream.of("+accumulo", "+default, 123").collect(Collectors.toList()));

    expect(mockZk.exists(anyString(), anyBoolean())).andReturn(new Stat()).anyTimes();

    Capture<String> mockPath = EasyMock.newCapture();
    expect(mockZk.getChildren(anyString(), anyBoolean())).andReturn(new ArrayList<>());
    expect(mockZk.setData(anyString(), anyObject(), anyInt())).andReturn(new Stat());

    replay(mockContext, mockZk);

    ConfigurationUpgrade upgrade = new ConfigurationUpgrade(mockContext, mockZk, true);

    upgrade.convertNamespaces();
  }

  private Stat makeStat(final String data) {
    long now = System.currentTimeMillis();
    Stat s = new Stat();
    s.setCzxid(0x46e5);
    s.setCtime(now);
    s.setMzxid(0x46e5);
    s.setMtime(now);
    s.setPzxid(0x46e5);
    s.setCversion(0);
    s.setVersion(0);
    s.setAversion(0);
    s.setEphemeralOwner(0x0);
    s.setDataLength(data.getBytes(UTF_8).length);
    s.setNumChildren(0);
    return s;
  }

  public void x() {

  }
}
