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
package org.apache.accumulo.server.conf2;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooPropStore extends PropStore {

  private static final Logger log = LoggerFactory.getLogger(ZooPropStore.class);

  private final ZooKeeper zookeeper;
  private final String instanceId;

  String tableConfRoot;

  public ZooPropStore(final ZooKeeper zookeeper, final String instanceId) {
    this.zookeeper = zookeeper;
    this.instanceId = instanceId;

    this.tableConfRoot = String.format("/accumulo/%s/tables", instanceId);
  }

  /**
   * Convert the table configuration properties from individual ZooKeeper nodes (Accumulo pre-2.1
   * format) into the single node (2.1) format. If the destination configuration node exists, it
   * will be overwritten.
   *
   * @param tableId
   *          the table id
   */
  public void upgrade(final TableId tableId) throws KeeperException, InterruptedException {

    String srcPath = String.format("%s/%s/conf", tableConfRoot, tableId.canonical());
    String destPath = String.format("%s/%s/conf2", tableConfRoot, tableId.canonical());

    Stat srcStat = zookeeper.exists(srcPath, false);

    if (Objects.nonNull(srcStat)) {

      PropEncoding props = copy(srcPath, srcStat.getPzxid());

      Stat destStat = zookeeper.exists(destPath, false);

      byte[] payload = props.toBytes();

      log.info("Props: {}", props.print(true));

      if (Objects.isNull(destStat)) {
        zookeeper.create(destPath, payload, ZooUtil.PRIVATE, CreateMode.PERSISTENT);
      } else {
        zookeeper.setData(destPath, payload, destStat.getVersion());
      }
    }
  }

  private PropEncoding copy(final String srcPath, final long pzxid)
      throws KeeperException, InterruptedException {

    PropEncoding props = new PropEncodingV1(1, true, Instant.now());

    List<String> configNodes = zookeeper.getChildren(srcPath, false);

    for (String propName : configNodes) {
      byte[] data = zookeeper.getData(srcPath + "/" + propName, null, null);
      props.addProperty(propName, new String(data, UTF_8));
    }

    // validate that config has not changed while processing child nodes.
    Stat stat = zookeeper.exists(srcPath, false);
    if (pzxid != stat.getPzxid()) {
      // this could also retry.
      throw new IllegalStateException(
          "Configuration nodes under " + srcPath + " changed while coping to new format.");
    }

    return props;
  }

  /**
   * Convert the table configuration properties from a single node into multiple nodes used by
   * Accumulo versions less than 2.1.
   *
   * @param tableId
   *          the table id
   */
  public void downgrade(final TableId tableId) {

  }
}
