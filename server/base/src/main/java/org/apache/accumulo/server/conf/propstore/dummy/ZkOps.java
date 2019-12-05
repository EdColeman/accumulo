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

import java.util.ArrayList;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf.propstore.proto2.cache.PropMap;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * dummy interface for mocking high-level zookeeper ops.
 */
public interface ZkOps {

  ArrayList<ACL> defaultAcl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

  CreateMode defaultMode = CreateMode.PERSISTENT;

  static String nodePathForTableId(final String rootPath, final TableId tableId) {
    return rootPath + '/' + tableId.canonical();
  }

  NodeData readNodeData(final String path, final Watcher watcher);

  void setNodeData(final String path, final byte[] data) throws AccumuloException;

  Stat getNodeStat(final String path, final Watcher watcher);

  // ? rename - to get or create?
  // NodeData getNodeData(final String path);

  Stat writeProps(final String path, final TableId tableId, final PropMap props)
      throws AccumuloException;

  ZooCallMetricsImpl getMetrics();
}
