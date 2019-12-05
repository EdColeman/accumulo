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
package org.apache.accumulo.server.conf.propstore.proto2.cache;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf.propstore.dummy.NodeData;
import org.apache.accumulo.server.conf.propstore.dummy.ZkOps;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropWriter {

  // TODO make class with json for parsing out reason (enum) and table id of change.
  public enum TableChangeReasons {
    TABLE_PROP_UPDATE, TABLE_DELETED, TABLE_ADDED
  };

  private static final Logger log = LoggerFactory.getLogger(PropWriter.class);

  private final ZkOps zoo;

  public PropWriter(final ZkOps zoo) {
    this.zoo = zoo;
  }

  public void update(final String rootPath, final TableId tableId, final Property prop,
      final String value) throws AccumuloException {

    String zPath = ZkOps.nodePathForTableId(rootPath, tableId);

    NodeData nodeData = zoo.readNodeData(zPath, null);

    log.debug("Node data on update: {}", nodeData);

    PropMap map;
    if (nodeData.isEmpty()) {
      map = new PropMap(tableId, nodeData.getStat());
    } else {
      map = PropMap.fromJson(nodeData.getData());
      map.setDataVersion(nodeData.getStat());
    }

    map.set(prop, value);

    updateAll(rootPath, tableId, map);
  }

  public void updateAll(final String rootPath, final TableId tableId, final PropMap props)
      throws AccumuloException {

    log.debug("Writing property map to zookeeper: {}", props);

    Stat stat = zoo.writeProps(rootPath, tableId, props);

    if (stat == null) {
      throw new AccumuloException(
          "Invalid property update, zookeeper write failed to return stat for node '"
              + ZkOps.nodePathForTableId(rootPath, tableId) + '\'');
    }

    log.info("Trigger watcher for " + rootPath);

    // update parent with change reason.
    zoo.setNodeData(rootPath,
        new String(TableChangeReasons.TABLE_PROP_UPDATE.name() + ": " + tableId.toString())
            .getBytes(UTF_8));
  }
}
