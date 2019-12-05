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

import static org.apache.accumulo.server.conf.propstore.ZooFunc.prettyStat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf.propstore.dummy.ZkOps;
import org.apache.accumulo.server.conf.propstore.dummy.ZkOpsImpl;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropMapZooLoaderTest {

  private static final Logger log = LoggerFactory.getLogger(PropMapZooLoaderTest.class);

  String rootPath = "/accumulo/1234/config";
  TableId tableId = TableId.of("abc1");

  /**
   * There is no zookeeper node - return an empty map.
   */
  @Test
  public void loadNoNode() throws Exception {

    ZooKeeper zooMock = EasyMock.createStrictMock(ZooKeeper.class);

    // call to get data without a node - throw NoNodeException
    EasyMock
        .expect(zooMock.getData(EasyMock.isA(String.class), EasyMock.anyBoolean(),
            EasyMock.isA(Stat.class)))
        .andThrow(new KeeperException.NoNodeException("path does not exist"));

    EasyMock.replay(zooMock);

    ZkOps zoo = new ZkOpsImpl(zooMock);

    PropMapZooLoader loader = new PropMapZooLoader(rootPath, zoo);

    PropMap m = loader.load(tableId);

    EasyMock.verify(zooMock);

    assertNotNull(m);
    assertEquals(tableId, m.getTableId());
    assertEquals(0, m.getStat().getPzxid());

    assertTrue(null, m.get(Property.TABLE_BLOOM_ENABLED).isEmpty());

  }

  @Test
  public void loadWithData() throws Exception {

    ZooKeeper zooMock = EasyMock.createStrictMock(ZooKeeper.class);

    // call to get data without a node - throw NoNodeException
    EasyMock.expect(zooMock.getData(EasyMock.isA(String.class), EasyMock.anyBoolean(),
        EasyMock.isA(Stat.class))).andAnswer(new IAnswer<byte[]>() {
          @Override
          public byte[] answer() throws Throwable {
            byte data[] = genData(tableId);
            Stat s = (Stat) EasyMock.getCurrentArguments()[2];
            s.setPzxid(123);
            s.setVersion(1);
            s.setDataLength(data.length);
            return data;
          }
        });

    EasyMock.replay(zooMock);

    ZkOps zoo = new ZkOpsImpl(zooMock);

    PropMapZooLoader loader = new PropMapZooLoader(rootPath, zoo);

    PropMap m = loader.load(tableId);

    EasyMock.verify(zooMock);

    assertNotNull(m);
    assertEquals(tableId, m.getTableId());

    log.info("Stat: {}", prettyStat(m.getStat()));

    assertEquals(123, m.getStat().getPzxid());

    assertEquals("true", m.get(Property.TABLE_BLOOM_ENABLED).get());
    assertEquals("99", m.get(Property.TABLE_FILE_MAX).get());

  }

  static byte[] genData(final TableId tableId) {

    PropMap m = new PropMap(tableId, new Stat());
    m.set(Property.TABLE_BLOOM_ENABLED, "true");
    m.set(Property.TABLE_FILE_MAX, "99");

    byte[] data = m.toJson();

    return m.toJson();
  }
}
