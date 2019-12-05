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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf.propstore.PropTestData;
import org.apache.accumulo.server.conf.propstore.dummy.ZkOps;
import org.apache.accumulo.server.conf.propstore.dummy.ZkOpsImpl;
import org.apache.accumulo.server.conf.propstore.proto2.SentinelMonitorTest;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropWriterMockTest {

  private static final Logger log = LoggerFactory.getLogger(SentinelMonitorTest.class);

  private static final String NODE_1_PATH = "/accumulo/1234/config/node-000001";
  public static final TableId TABLE_1_ID = TableId.of("a123");

  @Test
  public void update() throws Exception {

    Stat s = new Stat();

    ZooKeeper zoo = EasyMock.createStrictMock(ZooKeeper.class);

    EasyMock.expect(zoo.getData(EasyMock.anyString(), EasyMock.isNull(), EasyMock.isA(Stat.class)))
        .andAnswer(new IAnswer<byte[]>() {
          @Override
          public byte[] answer() {
            Stat stat = (Stat) EasyMock.getCurrentArguments()[2];
            s.setPzxid(stat.getPzxid());
            s.setVersion(stat.getVersion());
            byte[] data = PropTestData.getTableBytes(TABLE_1_ID, stat);
            stat.setDataLength(data.length);
            stat.setPzxid(s.getVersion() + 1);
            return PropTestData.getTableBytes(TABLE_1_ID, stat);
          }
        });

    EasyMock
        .expect(zoo.setData(EasyMock.anyString(), EasyMock.isA(byte[].class), EasyMock.anyInt()))
        .andAnswer(new IAnswer<Stat>() {
          @Override
          public Stat answer() {
            int v = s.getVersion();
            s.setVersion(v + 195);
            return s;
          }
        });

    EasyMock.expect(zoo.getData(EasyMock.anyString(), EasyMock.isNull(), EasyMock.isA(Stat.class)))
        .andAnswer(new IAnswer<byte[]>() {
          @Override
          public byte[] answer() {
            Stat stat = (Stat) EasyMock.getCurrentArguments()[2];
            s.setPzxid(stat.getPzxid());
            s.setVersion(stat.getVersion());
            byte[] data = PropTestData.getTableBytes(TABLE_1_ID, stat);
            stat.setDataLength(data.length);
            stat.setPzxid(s.getPzxid() + 1);
            return PropTestData.getTableBytes(TABLE_1_ID, stat);
          }
        });

    EasyMock.replay(zoo);

    ZkOps zkOps = new ZkOpsImpl(zoo);

    PropWriter propWriter = new PropWriter(zkOps);

    propWriter.update(NODE_1_PATH, TABLE_1_ID, Property.TABLE_BLOOM_ENABLED, "false");

    Thread.sleep(200);

    PropMapZooLoader loader = new PropMapZooLoader(NODE_1_PATH, zkOps);

    PropMap m = loader.load(TABLE_1_ID);

    assertEquals("true", m.get(Property.TABLE_BLOOM_ENABLED).get());

    log.debug("M:{}", m);

    EasyMock.verify(zoo);

  }

  @Test
  public void handleUpdateAddConflict() throws Exception {

    Stat s = new Stat();

    ZooKeeper zoo = EasyMock.createStrictMock(ZooKeeper.class);

    EasyMock.expect(zoo.getData(EasyMock.anyString(), EasyMock.isNull(), EasyMock.isA(Stat.class)))
        .andAnswer(new IAnswer<byte[]>() {
          @Override
          public byte[] answer() {
            Stat stat = (Stat) EasyMock.getCurrentArguments()[2];
            s.setPzxid(stat.getPzxid());
            s.setVersion(stat.getVersion());
            byte[] data = PropTestData.getTableBytes(TABLE_1_ID, stat);
            stat.setDataLength(data.length);
            stat.setPzxid(1);
            return PropTestData.getTableBytes(TABLE_1_ID, stat);
          }
        });

    EasyMock
        .expect(zoo.setData(EasyMock.anyString(), EasyMock.isA(byte[].class), EasyMock.anyInt()))
        .andThrow(new KeeperException.BadVersionException("bad version") {});

    EasyMock.replay(zoo);

    ZkOps zkOps = new ZkOpsImpl(zoo);

    PropWriter propWriter = new PropWriter(zkOps);

    try {
      propWriter.update(NODE_1_PATH, TABLE_1_ID, Property.TABLE_BLOOM_ENABLED, "false");
      fail("AccumuloException expected");
    } catch (AccumuloException ex) {
      // exception expected
    }

    EasyMock.verify(zoo);
  }

}
