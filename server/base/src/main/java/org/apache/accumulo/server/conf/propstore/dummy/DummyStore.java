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
package org.apache.accumulo.server.conf.propstore.dummy;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.accumulo.core.data.TableId;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// functional skeletons for prop store development.
public class DummyStore {

  private static final Logger log = LoggerFactory.getLogger(DummyStore.class);

  private final String rootPath;
  private final SNode parent;
  private final ZkOps zoo;

  private final Map<TableId,Stat> watchedTables = new HashMap<>();

  public DummyStore(final String rootPath, final SNode parent, final ZkOps zoo) {
    this.rootPath = rootPath;
    this.parent = parent;
    this.zoo = zoo;
  }

  public Map<String,String> loadTableProps(TableId tableId) {

    // if node exists - get it.
    NodeData nodeData = zoo.readNodeData(ZkOps.nodePathForTableId(rootPath, tableId), null);

    log.debug("ND: {}", nodeData);

    // update / add table id to nodes that are in use.
    Stat v = watchedTables.merge(tableId, nodeData.getStat(), updateIfLater);
    if (v != nodeData.getStat()) {
      log.info("Update rejected - version is older");
    }

    Map<String,String> tableProps = Collections.emptyMap();
    parent.setProps(tableId, tableProps);
    return tableProps;
  }

  BiFunction<Stat,Stat,Stat> updateIfLater =
      (prev, curr) -> (curr.getVersion() > prev.getVersion()) ? curr : prev;
}
