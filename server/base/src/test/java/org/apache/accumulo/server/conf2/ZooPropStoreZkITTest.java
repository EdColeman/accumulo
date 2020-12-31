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

import static org.junit.Assume.assumeTrue;

import java.util.List;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.accumulo.server.conf2.impl.ZooPropStore;
import org.apache.zookeeper.data.Stat;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooPropStore test that uses a live zookeeper instance.
 */
public class ZooPropStoreZkITTest extends ZooBase {

  private static final Logger log = LoggerFactory.getLogger(ZooPropStoreZkITTest.class);

  @BeforeClass
  public static void init() throws Exception {
    ZooBase.init();
  }

  @Test
  public void setTest() throws Exception {
    assumeTrue("Could not connect to zookeeper, skipping", haveZookeeper());

    List<String> nodes = getZooKeeper().getChildren(getBasePath(), false);

    log.debug("Nodes: {}", nodes);

    PropStore store =
        new ZooPropStore.Builder().withZk(getZooKeeper()).forInstance(getTestUuid()).build();

    PropEncoding storedProp = new PropEncodingV1();
    fillMap(storedProp);
    CacheId id = new CacheId(getTestUuid(), null, TableId.of("a1"));

    store.writeToStore(id, storedProp);

    // look into zookeeper
    Stat n = getZooKeeper().exists(getConfigPath() + "/" + id.nodeName(), false);
    log.debug("Zookeeper says: {}", ZooUtil.printStat(n));

    PropEncoding readProp = store.readFromStore(id);

    log.debug("Read: {}", readProp.print(true));

    readProp.addProperty("key1", "value-update-1");

    store.writeToStore(id, readProp);

    try {
      Thread.sleep(30_000);
    } catch (InterruptedException ex) {
      // empty.
    }
  }
}
