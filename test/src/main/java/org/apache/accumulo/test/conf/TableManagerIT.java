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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.PropCacheId;
import org.apache.accumulo.server.conf2.PropStoreException;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.tables.TableManager2;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableManagerIT extends SharedMiniClusterBase
    implements MiniClusterConfigurationCallback {

  private static final Logger log = LoggerFactory.getLogger(TableManagerIT.class);

  private static ServerContext context;
  private static String instanceId;

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    // cfg.setProperty("", "");
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
  public void constructor() {
    TableManager2 tableMgr2 = new TableManager2(context);
    assertNotNull(tableMgr2);
  }

  @Test
  public void prepareNewNamespaceState()
      throws InterruptedException, KeeperException, PropStoreException {

    NamespaceId ns2 = NamespaceId.of("ns2");

    // validate that prop node does not exists - expect node does not exist exception.
    try {
      context.getPropStore().get(PropCacheId.forNamespace(instanceId, ns2));
      fail("Expected no node exception");
    } catch (PropStoreException pex) {
      assertEquals(PropStoreException.REASON_CODE.NO_ZK_NODE, pex.getCode());
    }

    TableManager2.prepareNewNamespaceState(context, instanceId, ns2, "ns2",
        ZooUtil.NodeExistsPolicy.FAIL);

    assertNotNull(context.getPropStore().get(PropCacheId.forNamespace(instanceId, ns2)));
  }

  @Test
  public void prepareNewTableState()
      throws InterruptedException, KeeperException, PropStoreException {

    TableId tableId1 = TableId.of("t1");

    TableManager.prepareNewTableState(context.getZooReaderWriter(), instanceId, tableId1,
        Namespace.DEFAULT.id(), "table1", TableState.NEW, ZooUtil.NodeExistsPolicy.OVERWRITE);

    // validate that prop node does not exists - expect node does not exist exception.
    try {
      context.getPropStore().get(PropCacheId.forTable(instanceId, tableId1));
      fail("Expected no node exception");
    } catch (PropStoreException pex) {
      assertEquals(PropStoreException.REASON_CODE.NO_ZK_NODE, pex.getCode());
    }

    TableManager2.prepareNewTableState(context, instanceId, tableId1, Namespace.DEFAULT.id(),
        "table1", TableState.NEW, ZooUtil.NodeExistsPolicy.OVERWRITE);

    assertNotNull(context.getPropStore().get(PropCacheId.forTable(instanceId, tableId1)));
  }

  @Test
  public void prepareNewTableStateWithNamespace() throws InterruptedException, KeeperException {
    NamespaceId ns2 = NamespaceId.of("ns2");

    TableId tableId1 = TableId.of("t2");

    String tableName = "table2";

    TableManager.prepareNewTableState(context.getZooReaderWriter(), instanceId, tableId1, ns2,
        tableName, TableState.NEW, ZooUtil.NodeExistsPolicy.OVERWRITE);

    log.info("table name: {}", tableName);

  }

  @Test
  public void getTableState() {}

  @Test
  public void transitionTableState() {}

  @Test
  public void updateTableStateCache() {}

  @Test
  public void addTable() {}

  @Test
  public void cloneTable() {}

  @Test
  public void removeTable() {}

  @Test
  public void addObserver() {}

  @Test
  public void removeNamespace() {}

  private static class ARef {
    private String n;
  }

  @Test
  public void x() {
    ARef aRef = new ARef();
    aRef.n = "n1";
    changeMe(aRef);
    log.info("N:{}", aRef.n);
  }

  private void changeMe(final ARef ref) {
    ref.n = ref.n + "+added";
  }
}
