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
package org.apache.accumulo.test.conf;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.NamespaceConfiguration2;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.conf.ServerConfiguration2;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.ServerConfigurationFactory2;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.conf.TableConfiguration2;
import org.apache.accumulo.server.conf2.PropCacheId1;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemConfigurationIT extends ConfigurableMacBase {

  private static final Logger log = LoggerFactory.getLogger(SystemConfigurationIT.class);

  private final Predicate<String> all = (String s) -> true;
  private ServerContext context;

  private ZooKeeper zooKeeper;

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.GC_METRICS_ENABLED, "true");
  }

  @Before
  public void clusterInit() {
    context = cluster.getServerContext();

    populateZooKeeper(context);

  }

  private void populateZooKeeper(ServerContext context) {

    try {

      // String iid = context.getInstanceID();
      //
      // // todo - should be replace by system initialization / upgrade
      zooKeeper = new ZooKeeper(context.getZooKeepers(), 10_000, new TestWatcher());
      zooKeeper.addAuthInfo("digest",
          ("accumulo:" + cluster.getSiteConfiguration().get(Property.INSTANCE_SECRET))
              .getBytes(UTF_8));
      // zooKeeper.create("/accumulo/" + iid + "/pe_config2", new byte[0], ZooUtil.PUBLIC,
      // CreateMode.PERSISTENT);

      // context.getPropStore().create(PropCacheId1.forSystem(iid), null);

    } catch (Exception ex) {
      throw new IllegalStateException(ex);
    }
  }

  @Test
  public void defaultSystemProps() {

    ServerConfiguration config1 =
        new ServerConfigurationFactory(context, context.getSiteConfiguration());
    log.info("Old config: {}", config1);
    Map<String,String> p1 = new HashMap<>();
    config1.getSystemConfiguration().getProperties(p1, all);
    log.info("Old system config: {}", p1);

    ServerConfiguration2 config2 = new ServerConfigurationFactory2(context);

    log.info("My config: {}", config2);

    Map<String,String> p2 = new HashMap<>();
    config2.getSystemConfiguration().getProperties(p2, all);

    log.info("My config: {}", p2);

    log.info("Size: {} == {}", p1.size(), p2.size());

    assertEquals(p1, p2);

  }

  @Test
  public void namespaceConfig() throws Exception {
    NamespaceId ns1Id = NamespaceId.of("ns1");
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      c.namespaceOperations().create(ns1Id.canonical());
    }

    PropCacheId1 ns1 = PropCacheId1.forNamespace(context.getInstanceID(), ns1Id);
    PropEncoding defaultNsProps = new PropEncodingV1();
    zooKeeper.create(ns1.path(), defaultNsProps.toBytes(), ZooUtil.PUBLIC, CreateMode.PERSISTENT);

    ServerConfiguration config1 =
        new ServerConfigurationFactory(context, context.getSiteConfiguration());
    log.info("Old namespace: {}", config1);
    Map<String,String> n1 = new HashMap<>();
    config1.getSystemConfiguration().getProperties(n1, all);
    log.info("Old namespace config: {}", n1);

    ServerConfiguration2 config2 = new ServerConfigurationFactory2(context);

    log.info("My config: {}", config2);

    Map<String,String> p2 = new HashMap<>();
    config2.getSystemConfiguration().getProperties(p2, all);

    log.info("My config: {}", p2);
    NamespaceConfiguration2 nsConfig2 = config2.getNamespaceConfiguration(ns1Id);
    String v = nsConfig2.get("alice");

    log.info("v: {}", v);
    Map<String,String> n2 = new HashMap<>();
    nsConfig2.getProperties(n2, all);
    log.info("all: {} - {}", n2.size(), n2);
  }

  @Test
  public void tableConfig() throws Exception {

    TableId tid1;
    NamespaceId namespaceId;
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      c.tableOperations().create("table_a");
      tid1 = TableId.of(c.tableOperations().tableIdMap().get("table_a"));

      log.info("namespace ids: {}", c.namespaceOperations().namespaceIdMap());
      log.info("what: {}", Tables.getNamespaceId(context, tid1));

      namespaceId = Tables.getNamespaceId(context, tid1);

      log.info("Created namespace: {}", namespaceId);

    }

    PropCacheId1 nsDefaultId = PropCacheId1.forNamespace(context.getInstanceID(), namespaceId);
    PropCacheId1 t1CID = PropCacheId1.forTable(context.getInstanceID(), tid1);

    log.info("table config creating ids and nodes. ns: {}, t1 {}", nsDefaultId, t1CID);

    PropEncoding defaultProps = new PropEncodingV1();
    zooKeeper.create(nsDefaultId.path(), defaultProps.toBytes(), ZooUtil.PUBLIC,
        CreateMode.PERSISTENT);
    zooKeeper.create(t1CID.path(), defaultProps.toBytes(), ZooUtil.PUBLIC, CreateMode.PERSISTENT);

    ServerConfiguration config1 =
        new ServerConfigurationFactory(context, context.getSiteConfiguration());

    Map<String,String> oldSysProps = new HashMap<>();
    config1.getSystemConfiguration().getProperties(oldSysProps, all);
    log.info("Old sys config: {}", oldSysProps);
    TableConfiguration tConfig1 = config1.getTableConfiguration(tid1);

    Map<String,String> oldProps = new HashMap<>();
    tConfig1.getProperties(oldProps, all);
    log.info("Table config old: {}", oldProps);

    ServerConfiguration2 config2 = new ServerConfigurationFactory2(context);

    log.info("My config: {}", config2);

    Map<String,String> p2 = new HashMap<>();

    log.info("Table was created: {}", Tables.exists(context, tid1));

    log.info("My config: {}", p2);
    TableConfiguration2 t1Config2 = config2.getTableConfiguration(tid1);
    String v = t1Config2.get("alice");

    log.info("v: {}", v);
    Map<String,String> t2 = new HashMap<>();
    t1Config2.getProperties(t2, all);
    log.info("all: {} - {}", t2.size(), t2);

  }

  @Test
  public void tableWithNamespace() throws Exception {

    TableId tid1;
    NamespaceId namespaceId;
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      c.namespaceOperations().create("ns1");
      c.namespaceOperations().create("ns2");
      c.tableOperations().create("ns1.table_a");

      tid1 = TableId.of(c.tableOperations().tableIdMap().get("ns1.table_a"));

      log.info("namespace ids: {}", c.namespaceOperations().namespaceIdMap());

      log.info("what namespace id: {}", Tables.getNamespaceId(context, tid1));
      log.info("what table id: {}", tid1.canonical());

      namespaceId = Tables.getNamespaceId(context, tid1);

      try {
        c.tableOperations().rename("ns1.table_a", "ns2.table_a");
        fail("Expected an AccumuloException - cannot move namespace");
      } catch (AccumuloException ex) {
        // confirm that namespace cannot move namespace by renaming
      }
    }

  }

  private static class TestWatcher implements Watcher {

    @Override
    public void process(WatchedEvent watchedEvent) {

    }
  }
}
