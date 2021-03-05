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
package org.apache.accumulo.test.config2;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.accumulo.server.conf2.util.ConfigurationUpgrade;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationUpgradeZkTest {

  public static final String INSTANCE_ID = UUID.randomUUID().toString();

  @SuppressWarnings("deprecation")
  public static final String ZK_SRC_ROOT_PATH = "/accumulo/" + INSTANCE_ID + Constants.ZCONFIG;

  public static final String ZK_SRC_NAMESPACE_PATH =
      "/accumulo/" + INSTANCE_ID + Constants.ZNAMESPACES;

  public static final String ZK_SRC_TABLE_PATH = "/accumulo/" + INSTANCE_ID + Constants.ZTABLES;

  public static final String ZK_DEST_ROOT_PATH =
      "/accumulo/" + INSTANCE_ID + Constants.ZENCODED_CONFIG_ROOT;

  private static final Logger log = LoggerFactory.getLogger(ConfigurationUpgradeZkTest.class);

  private static ZooKeeperTestingServer szk = null;
  private static ZooKeeper zooKeeper;

  @BeforeClass
  public static void setupZk() {
    // using default zookeeper port - we don't have a full configuration
    szk = new ZooKeeperTestingServer();

    zooKeeper = szk.getZooKeeper();
  }

  @AfterClass
  public static void shutdownZK() throws Exception {
    szk.close();
  }

  @Before
  public void setupZnodes() {
    szk.initPaths(ZK_DEST_ROOT_PATH);
    szk.initPaths(ZK_SRC_ROOT_PATH);
    szk.initPaths(ZK_SRC_NAMESPACE_PATH);
    szk.initPaths(ZK_SRC_TABLE_PATH);
  }

  @After
  public void removeZnodes() {

    try {

      ZKUtil.deleteRecursive(zooKeeper, "/accumulo");

    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to clean-up test zooKeeper nodes.", ex);
    }
  }

  @Test
  public void emptySystemConfigTest() throws Exception {

    ServerContext mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();

    replay(mockContext);

    ConfigurationUpgrade configUpgrade = new ConfigurationUpgrade(mockContext, zooKeeper, true);

    configUpgrade.convertSystem();

    Set<String> children = new HashSet<>(zooKeeper.getChildren(ZK_DEST_ROOT_PATH, false));
    assertTrue(children.size() >= 1);
    for (String s : children) {
      log.info("nodes: {}", s);
    }

    var sysId = CacheId.forSystem(mockContext);
    var expectedNodeName = sysId.path().substring(sysId.path().lastIndexOf('/') + 1);
    assertTrue(children.contains(expectedNodeName));

    assertTrue(configUpgrade.verifyById(CacheId.forSystem(mockContext)));

  }

  @Test
  public void emptyNamespace() throws Exception {

    ServerContext mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();

    replay(mockContext);

    ConfigurationUpgrade configUpgrade = new ConfigurationUpgrade(mockContext, zooKeeper, true);

    configUpgrade.convertNamespaces();

    Set<String> children = new HashSet<>(zooKeeper.getChildren(ZK_DEST_ROOT_PATH, false));
    assertEquals(0, children.size());
    for (String s : children) {
      log.info("nodes: {}", s);
    }

    assertTrue(configUpgrade.verifyById(CacheId.forNamespace(mockContext, NamespaceId.of("none"))));
  }

  @Test
  public void emptyTableConfigTest() throws Exception {

    ServerContext mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();

    replay(mockContext);

    ConfigurationUpgrade configUpgrade = new ConfigurationUpgrade(mockContext, zooKeeper, true);

    configUpgrade.convertTables();

    Set<String> children = new HashSet<>(zooKeeper.getChildren(ZK_DEST_ROOT_PATH, false));
    assertEquals(0, children.size());
    for (String s : children) {
      log.info("nodes: {}", s);
    }

  }

  @Test
  public void convertSystemConfigTest() throws Exception {

    Set<TestProp> srcProps = setProps(ZK_SRC_ROOT_PATH);

    ServerContext mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();

    replay(mockContext);

    ConfigurationUpgrade configUpgrade = new ConfigurationUpgrade(mockContext, zooKeeper, true);

    configUpgrade.convertSystem();

    // read dest props / decode and validate
    CacheId id = CacheId.forSystem(mockContext);

    Set<String> inZooKeeper = new HashSet<>(zooKeeper.getChildren(ZK_DEST_ROOT_PATH, false));
    assertTrue(inZooKeeper.size() >= 1);
    for (String s : inZooKeeper) {
      log.info("nodes: {}", s);
    }

    // basic test - dest node created.
    var expectedNodeName = id.path().substring(id.path().lastIndexOf('/') + 1);
    assertTrue(inZooKeeper.contains(expectedNodeName));

    validateProps(srcProps, id);

    assertTrue(configUpgrade.verifyById(CacheId.forSystem(mockContext)));
  }

  @Test
  public void convertNamespace() throws Exception {

    szk.initPaths(ZK_SRC_NAMESPACE_PATH + "/+accumulo/conf");
    szk.initPaths(ZK_SRC_NAMESPACE_PATH + "/+default/conf");
    szk.initPaths(ZK_SRC_NAMESPACE_PATH + "/3/conf");

    Set<TestProp> expected = setProps(ZK_SRC_NAMESPACE_PATH + "/3/conf");

    ServerContext mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();

    replay(mockContext);

    ConfigurationUpgrade configUpgrade = new ConfigurationUpgrade(mockContext, zooKeeper, true);

    configUpgrade.convertNamespaces();

    CacheId nsAccId = CacheId.forNamespace(mockContext, NamespaceId.of("+accumulo"));
    validateProps(Collections.emptySet(), nsAccId);

    CacheId nsDefault3Id = CacheId.forNamespace(mockContext, NamespaceId.of("+default"));
    validateProps(Collections.emptySet(), nsDefault3Id);

    CacheId ns3Id = CacheId.forNamespace(mockContext, NamespaceId.of("3"));
    validateProps(expected, ns3Id);

    try {
      CacheId badId = CacheId.forNamespace(mockContext, NamespaceId.of("NoNode"));
      validateProps(Collections.emptySet(), badId);
      fail("Expected KeeperException.NoNode for nonexistent namespace node");
    } catch (KeeperException.NoNodeException ex) {
      // expected
    }
  }

  @Test
  public void convertTables() throws Exception {

    szk.initPaths(ZK_SRC_TABLE_PATH + "/!0/conf");
    szk.initPaths(ZK_SRC_TABLE_PATH + "/+r/conf");
    szk.initPaths(ZK_SRC_TABLE_PATH + "/+rep/conf");
    szk.initPaths(ZK_SRC_TABLE_PATH + "/1/conf");
    szk.initPaths(ZK_SRC_TABLE_PATH + "/6/conf");

    Set<TestProp> expected = setProps(ZK_SRC_TABLE_PATH + "/6/conf");

    ServerContext mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();

    replay(mockContext);

    ConfigurationUpgrade configUpgrade = new ConfigurationUpgrade(mockContext, zooKeeper, true);

    configUpgrade.convertTables();

    validateProps(Collections.emptySet(), CacheId.forTable(mockContext, TableId.of("!0")));
    validateProps(Collections.emptySet(), CacheId.forTable(mockContext, TableId.of("+r")));
    validateProps(Collections.emptySet(), CacheId.forTable(mockContext, TableId.of("+rep")));
    validateProps(Collections.emptySet(), CacheId.forTable(mockContext, TableId.of("1")));

    CacheId tid6 = CacheId.forTable(mockContext, TableId.of("6"));
    validateProps(expected, tid6);

    assertTrue(configUpgrade.verifyById(CacheId.forTable(mockContext, TableId.of("6"))));

    try {
      CacheId badId = CacheId.forTable(mockContext, TableId.of("NoTable"));
      validateProps(Collections.emptySet(), badId);
      fail("Expected KeeperException.NoNode for nonexistent namespace node");
    } catch (KeeperException.NoNodeException ex) {
      // expected
    }
  }

  /**
   * Validate a complete conversion
   */
  @Test
  public void completeConversionTest() {
    ServerContext mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();

    replay(mockContext);

    ConfigurationUpgrade configUpgrade = new ConfigurationUpgrade(mockContext, zooKeeper, true);
    configUpgrade.performUpgrade();

    assertTrue(configUpgrade.verifyAll());

  }

  private Set<TestProp> setProps(final String configPath)
      throws KeeperException, InterruptedException {

    Set<TestProp> props = new HashSet<>();
    props.add(new TestProp("table.split.threshold", "512M"));
    props.add(new TestProp("tserver.walog.max.referenced", "5"));

    for (TestProp p : props) {
      zooKeeper.create(configPath + "/" + p.getName(), p.getData(), ZooUtil.PUBLIC,
          CreateMode.PERSISTENT);
    }
    return props;
  }

  private void validateProps(Set<TestProp> srcProps, CacheId id)
      throws KeeperException, InterruptedException {

    byte[] data = zooKeeper.getData(id.path(), null, null);
    PropEncodingV1 ep = new PropEncodingV1(data);
    Map<String,String> destProps = ep.getAllProperties();

    for (TestProp p : srcProps) {
      assertTrue(destProps.containsKey(p.getName()));
      assertEquals(new String(p.getData()), destProps.get(p.getName()));
    }
  }

  private static class TestProp {

    final String name;
    final byte[] data;

    public TestProp(final String name, final String data) {
      this.name = name;
      this.data = data.getBytes(UTF_8);
    }

    public String getName() {
      return name;
    }

    public byte[] getData() {
      return data;
    }
  }
}
