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

import static org.junit.Assert.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.conf2.PropCacheId;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemConfigurationIT extends SharedMiniClusterBase
    implements MiniClusterConfigurationCallback {

  private static final Logger log = LoggerFactory.getLogger(SystemConfigurationIT.class);
  // simple way to create test unique ids that can be used a table / namespace ids.
  private final static AtomicInteger nextId = new AtomicInteger(20);
  private static String instanceId;
  private final Predicate<String> all = (String s) -> true;
  private ServerContext context;
  private ZooKeeper zooKeeper;

  private static String idGen() {
    return "" + nextId.incrementAndGet();
  }

  private static String nameGen() {
    return "name" + nextId.incrementAndGet();
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    cfg.setProperty(Property.GC_METRICS_ENABLED, "true");
  }

  @Before
  public void clusterInit() throws Exception {
    if (SharedMiniClusterBase.getCluster() == null) {
      SharedMiniClusterBase.startMiniClusterWithConfig(this);
    }

    context = SharedMiniClusterBase.getCluster().getServerContext();
    instanceId = context.getInstanceID();

  }

  @Test
  public void defaultSystemProps() {

    ServerConfiguration config1 =
        new ServerConfigurationFactory(context, context.getSiteConfiguration());
    log.info("Old config: {}", config1);
    Map<String,String> p1 = new HashMap<>();
    config1.getSystemConfiguration().getProperties(p1, all);
    log.info("Old system config: {}", p1);
  }

  @Test
  public void namespaceConfig() throws Exception {
    NamespaceId ns1Id = NamespaceId.of(nameGen());

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.namespaceOperations().create(ns1Id.canonical());
    }

    ServerConfiguration config1 =
        new ServerConfigurationFactory(context, context.getSiteConfiguration());
    log.info("Old namespace: {}", config1);
    Map<String,String> n1 = new HashMap<>();
    config1.getSystemConfiguration().getProperties(n1, all);
    log.info("Old namespace config: {}", n1);

  }

  @Test
  public void tableConfig() throws Exception {

    TableId tid1;
    NamespaceId namespaceId;
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = "table_" + idGen();
      c.tableOperations().create(tableName);
      tid1 = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      log.info("namespace ids: {}", c.namespaceOperations().namespaceIdMap());
      log.info("what: {}", Tables.getNamespaceId(context, tid1));

      namespaceId = Tables.getNamespaceId(context, tid1);

      log.info("Created namespace: {}", namespaceId);

    }

    PropCacheId nsDefaultId = PropCacheId.forNamespace(context.getInstanceID(), namespaceId);
    PropCacheId t1CID = PropCacheId.forTable(context.getInstanceID(), tid1);

    log.info("table config creating ids and nodes. ns: {}, t1 {}", nsDefaultId, t1CID);

    ServerConfiguration config1 =
        new ServerConfigurationFactory(context, context.getSiteConfiguration());

    Map<String,String> oldSysProps = new HashMap<>();
    config1.getSystemConfiguration().getProperties(oldSysProps, all);
    log.info("Old sys config: {}", oldSysProps);
    TableConfiguration tConfig1 = config1.getTableConfiguration(tid1);

    Map<String,String> oldProps = new HashMap<>();
    tConfig1.getProperties(oldProps, all);
    log.info("Table config old: {}", oldProps);

  }

  @Test
  public void tableWithNamespace() throws Exception {

    TableId tid1;

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String ns1 = "ns" + idGen();
      c.namespaceOperations().create(ns1);

      String ns2 = "ns" + idGen();
      c.namespaceOperations().create(ns2);

      String tableName = "table_" + idGen();
      String qualifiedName1 = ns1 + "." + tableName;
      c.tableOperations().create(qualifiedName1);

      tid1 = TableId.of(c.tableOperations().tableIdMap().get(qualifiedName1));

      log.info("namespace ids: {}", c.namespaceOperations().namespaceIdMap());

      log.info("what namespace id: {}", Tables.getNamespaceId(context, tid1));
      log.info("what table id: {}", tid1.canonical());

      NamespaceId namespaceId = Tables.getNamespaceId(context, tid1);

      String qualifiedName2 = ns2 + "." + tableName;

      assertThrows(AccumuloException.class,
          () -> c.tableOperations().rename(qualifiedName1, qualifiedName2));

    }

  }

  private static class TestWatcher implements Watcher {

    @Override
    public void process(WatchedEvent watchedEvent) {

    }
  }
}
