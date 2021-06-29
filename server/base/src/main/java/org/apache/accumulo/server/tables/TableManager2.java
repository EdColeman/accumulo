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
package org.apache.accumulo.server.tables;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.PropCacheId;
import org.apache.accumulo.server.conf2.PropStore;
import org.apache.accumulo.server.conf2.PropStoreException;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class TableManager2 {

  private static final Logger log = LoggerFactory.getLogger(TableManager2.class);
  private static final Set<TableObserver> observers = Collections.synchronizedSet(new HashSet<>());
  private static final Map<TableId,TableState> tableStateCache =
      Collections.synchronizedMap(new HashMap<>());
  private static final byte[] ZERO_BYTE = {'0'};

  private final ServerContext context;
  private final String zkRoot;
  private final String instanceID;
  private final ZooReaderWriter zoo;
  private final ZooCache zooStateCache;

  public TableManager2(ServerContext context) {
    this.context = context;
    zkRoot = context.getZooKeeperRoot();
    instanceID = context.getInstanceID();
    zoo = context.getZooReaderWriter();
    zooStateCache = new ZooCache(zoo, new TableStateWatcher());
    updateTableStateCache();
  }

  public static void prepareNewNamespaceState(final ServerContext context, final String instanceId,
      final NamespaceId namespaceId, final String namespace, final NodeExistsPolicy existsPolicy)
      throws KeeperException, InterruptedException {
    log.debug("Creating ZooKeeper entries for new namespace {} (ID: {})", namespace, namespaceId);
    String zPath = Constants.ZROOT + "/" + instanceId + Constants.ZNAMESPACES + "/" + namespaceId;

    ZooReaderWriter zoo = context.getZooReaderWriter();

    zoo.putPersistentData(zPath, new byte[0], existsPolicy);
    zoo.putPersistentData(zPath + Constants.ZNAMESPACE_NAME, namespace.getBytes(UTF_8),
        existsPolicy);

    try {
      context.getPropStore().create(PropCacheId.forNamespace(instanceId, namespaceId), null);
    } catch (PropStoreException ex) {
      // TODO exception handling?
      throw new IllegalStateException(
          "Failed to prepare namespace for " + namespace + " (" + namespaceId.canonical() + ")",
          ex);
    }
  }

  public static void prepareNewTableState(final ServerContext context, final String instanceId,
      final TableId tableId, final NamespaceId namespaceId, String tableName,
      final TableState state, final NodeExistsPolicy existsPolicy)
      throws KeeperException, InterruptedException {
    // state gets created last
    log.debug("Creating ZooKeeper entries for new table {} (ID: {}) in namespace (ID: {})",
        tableName, tableId, namespaceId);
    Pair<String,String> qualifiedTableName = Tables.qualify(tableName);
    tableName = qualifiedTableName.getSecond();

    ZooReaderWriter zoo = context.getZooReaderWriter();

    String zTablePath = Constants.ZROOT + "/" + instanceId + Constants.ZTABLES + "/" + tableId;
    zoo.putPersistentData(zTablePath, new byte[0], existsPolicy);

    zoo.putPersistentData(zTablePath + Constants.ZTABLE_NAMESPACE,
        namespaceId.canonical().getBytes(UTF_8), existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_NAME, tableName.getBytes(UTF_8),
        existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_FLUSH_ID, ZERO_BYTE, existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_COMPACT_ID, ZERO_BYTE, existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_COMPACT_CANCEL_ID, ZERO_BYTE, existsPolicy);
    zoo.putPersistentData(zTablePath + Constants.ZTABLE_STATE, state.name().getBytes(UTF_8),
        existsPolicy);

    try {
      context.getPropStore().create(PropCacheId.forTable(instanceId, tableId), null);
    } catch (PropStoreException ex) {
      throw new IllegalStateException(
          "Failed to prepare entries for table " + tableName + " (" + tableId.canonical() + ")",
          ex);
    }
  }

  public TableState getTableState(TableId tableId) {
    return tableStateCache.get(tableId);
  }

  public synchronized void transitionTableState(final TableId tableId, final TableState newState) {
    Preconditions.checkArgument(newState != TableState.UNKNOWN);
    String statePath = zkRoot + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE;

    try {
      zoo.mutateOrCreate(statePath, newState.name().getBytes(UTF_8), oldData -> {
        TableState oldState = TableState.UNKNOWN;
        if (oldData != null)
          oldState = TableState.valueOf(new String(oldData, UTF_8));

        // this check makes the transition operation idempotent
        if (oldState == newState)
          return null; // already at desired state, so nothing to do

        boolean transition = true;
        // +--------+
        // v |
        // NEW -> (ONLINE|OFFLINE)+--- DELETING
        switch (oldState) {
          case NEW:
            transition = (newState == TableState.OFFLINE || newState == TableState.ONLINE);
            break;
          case ONLINE: // fall-through intended
          case UNKNOWN:// fall through intended
          case OFFLINE:
            transition = (newState != TableState.NEW);
            break;
          case DELETING:
            // Can't transition to any state from DELETING
            transition = false;
            break;
        }
        if (!transition)
          throw new IllegalTableTransitionException(oldState, newState);
        log.debug("Transitioning state for table {} from {} to {}", tableId, oldState, newState);
        return newState.name().getBytes(UTF_8);
      });
    } catch (Exception e) {
      log.error("FATAL Failed to transition table to state {}", newState);
      throw new RuntimeException(e);
    }
  }

  private void updateTableStateCache() {
    synchronized (tableStateCache) {
      for (String tableId : zooStateCache.getChildren(zkRoot + Constants.ZTABLES))
        if (zooStateCache.get(zkRoot + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE)
            != null)
          updateTableStateCache(TableId.of(tableId));
    }
  }

  public TableState updateTableStateCache(TableId tableId) {
    synchronized (tableStateCache) {
      TableState tState = TableState.UNKNOWN;
      byte[] data =
          zooStateCache.get(zkRoot + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE);
      if (data != null) {
        String sState = new String(data, UTF_8);
        try {
          tState = TableState.valueOf(sState);
        } catch (IllegalArgumentException e) {
          log.error("Unrecognized state for table with tableId={}: {}", tableId, sState);
        }
        tableStateCache.put(tableId, tState);
      }
      return tState;
    }
  }

  public void addTable(TableId tableId, NamespaceId namespaceId, String tableName)
      throws KeeperException, InterruptedException, NamespaceNotFoundException {
    prepareNewTableState(context, instanceID, tableId, namespaceId, tableName, TableState.NEW,
        NodeExistsPolicy.OVERWRITE);
    updateTableStateCache(tableId);
  }

  public void cloneTable(final TableId srcTableId, final TableId destTableId,
      final String tableName, final NamespaceId namespaceId,
      final Map<String,String> propertiesToSet, final Set<String> propertiesToExclude)
      throws KeeperException, InterruptedException {

    prepareNewTableState(context, instanceID, destTableId, namespaceId, tableName, TableState.NEW,
        NodeExistsPolicy.OVERWRITE);

    try {

      PropStore propStore = context.getPropStore();

      PropEncoding srcProps = propStore.get(PropCacheId.forTable(instanceID, srcTableId));

      Map<String,String> destProps = new HashMap<>(srcProps.getAllProperties());

      if (Objects.nonNull(propertiesToSet)) {
        destProps.putAll(propertiesToSet);
      }

      if (Objects.nonNull(propertiesToExclude)) {
        propertiesToExclude.forEach(destProps::remove);
      }

      // check properties are valid - remove and log if invalid, then continue with remaining set.
      Set<String> invalids = new HashSet<>();
      destProps.forEach((k, v) -> {
        if (!TablePropUtil.isPropertyValid(k, v)) {
          log.info("Removing invalid property {}:{} from clone to {} properties", k, v, tableName);
          invalids.add(k);
        }
      });

      destProps.keySet().removeAll(invalids);

      propStore.update(PropCacheId.forTable(instanceID, destTableId), destProps);

    } catch (PropStoreException ex) {
      // TODO evalute additional exception handling
      throw new IllegalStateException("Failed to clone table properties from "
          + srcTableId.canonical() + " to " + tableName + " (" + destTableId.canonical() + ")", ex);
    }

    updateTableStateCache(destTableId);
  }

  public void removeTable(TableId tableId) throws KeeperException, InterruptedException {
    synchronized (tableStateCache) {
      tableStateCache.remove(tableId);
      zoo.recursiveDelete(zkRoot + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_STATE,
          NodeMissingPolicy.SKIP);
      zoo.recursiveDelete(zkRoot + Constants.ZTABLES + "/" + tableId, NodeMissingPolicy.SKIP);
    }
  }

  public boolean addObserver(TableObserver to) {
    synchronized (observers) {
      synchronized (tableStateCache) {
        to.initialize();
        return observers.add(to);
      }
    }
  }

  public void removeNamespace(NamespaceId namespaceId)
      throws KeeperException, InterruptedException {
    zoo.recursiveDelete(zkRoot + Constants.ZNAMESPACES + "/" + namespaceId, NodeMissingPolicy.SKIP);
  }

  private class TableStateWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      if (log.isTraceEnabled()) {
        log.trace("{}", event);
      }
      final String zPath = event.getPath();
      final EventType zType = event.getType();

      String tablesPrefix = zkRoot + Constants.ZTABLES;
      TableId tableId = null;

      if (zPath != null && zPath.startsWith(tablesPrefix + "/")) {
        String suffix = zPath.substring(tablesPrefix.length() + 1);
        if (suffix.contains("/")) {
          String[] sa = suffix.split("/", 2);
          if (Constants.ZTABLE_STATE.equals("/" + sa[1]))
            tableId = TableId.of(sa[0]);
        }
        if (tableId == null) {
          log.warn("Unknown path in {}", event);
          return;
        }
      }

      switch (zType) {
        case NodeChildrenChanged:
          if (zPath != null && zPath.equals(tablesPrefix)) {
            updateTableStateCache();
          } else {
            log.warn("Unexpected path {}", zPath);
          }
          break;
        case NodeCreated:
        case NodeDataChanged:
          // state transition
          TableState tState = updateTableStateCache(tableId);
          log.debug("State transition to {} @ {}", tState, event);
          synchronized (observers) {
            for (TableObserver to : observers)
              to.stateChanged(tableId, tState);
          }
          break;
        case NodeDeleted:
          if (zPath != null && tableId != null
              && (zPath.equals(tablesPrefix + "/" + tableId + Constants.ZTABLE_STATE)
                  || zPath.equals(tablesPrefix + "/" + tableId + Constants.ZTABLE_CONF)
                  || zPath.equals(tablesPrefix + "/" + tableId + Constants.ZTABLE_NAME)))
            tableStateCache.remove(tableId);
          break;
        case None:
          switch (event.getState()) {
            case Expired:
              log.trace("Session expired {}", event);
              synchronized (observers) {
                for (TableObserver to : observers)
                  to.sessionExpired();
              }
              break;
            case SyncConnected:
            default:
              log.trace("Ignored {}", event);
          }
          break;
        default:
          log.warn("Unandled {}", event);
      }
    }
  }

}
