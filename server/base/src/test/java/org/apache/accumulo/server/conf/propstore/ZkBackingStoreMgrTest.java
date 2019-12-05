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
package org.apache.accumulo.server.conf.propstore;

import static org.junit.Assert.assertNotNull;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkBackingStoreMgrTest {

  private static final Logger log = LoggerFactory.getLogger(ZkBackingStoreMgrTest.class);

  @Test
  public void factoryGoPathTest() throws Exception {
    ZooKeeper mockZooKeeper = EasyMock.createStrictMock(ZooKeeper.class);

    // first call will check for /accumulo/[instance]
    EasyMock.expect(mockZooKeeper.exists(EasyMock.isA(String.class), EasyMock.anyBoolean()))
        .andAnswer(new IAnswer<Stat>() {
          @Override
          public Stat answer() throws Throwable {
            return new Stat();
          }
        });

    // next call will check for /accumulo/[instance]/config
    EasyMock.expect(mockZooKeeper.exists(EasyMock.isA(String.class), EasyMock.anyBoolean()))
        .andAnswer(new IAnswer<Stat>() {
          @Override
          public Stat answer() throws Throwable {
            return new Stat();
          }
        });

    EasyMock.replay(mockZooKeeper);

    ZkBackingStoreMgr store = new ZkBackingStoreMgr.Factory().forAccumuloInstance("1234")
        .withZooKeeper(mockZooKeeper).build();

    assertNotNull(store);

    EasyMock.verify(mockZooKeeper);
  }

  @Test(expected = IllegalArgumentException.class)
  public void factoryInvalidInstanceTest() throws Exception {
    ZooKeeper mockZooKeeper = EasyMock.createStrictMock(ZooKeeper.class);

    // first call will check for /accumulo/[instance]
    EasyMock.expect(mockZooKeeper.exists(EasyMock.isA(String.class), EasyMock.anyBoolean()))
        .andAnswer(new IAnswer<Stat>() {
          @Override
          public Stat answer() throws Throwable {
            return null;
          }
        });

    EasyMock.replay(mockZooKeeper);

    ZkBackingStoreMgr store = new ZkBackingStoreMgr.Factory().forAccumuloInstance("1234")
        .withZooKeeper(mockZooKeeper).build();

  }

  @Test(expected = IllegalArgumentException.class)
  public void factoryNoConfigTest() throws Exception {
    ZooKeeper mockZooKeeper = EasyMock.createStrictMock(ZooKeeper.class);

    // first call will check for /accumulo/[instance]
    EasyMock.expect(mockZooKeeper.exists(EasyMock.isA(String.class), EasyMock.anyBoolean()))
        .andAnswer(new IAnswer<Stat>() {
          @Override
          public Stat answer() throws Throwable {
            return new Stat();
          }
        });

    // next call will check for /accumulo/[instance]/config
    EasyMock.expect(mockZooKeeper.exists(EasyMock.isA(String.class), EasyMock.anyBoolean()))
        .andAnswer(new IAnswer<Stat>() {
          @Override
          public Stat answer() throws Throwable {
            return null;
          }
        });

    EasyMock.replay(mockZooKeeper);

    ZkBackingStoreMgr store = new ZkBackingStoreMgr.Factory().forAccumuloInstance("1234")
        .withZooKeeper(mockZooKeeper).build();

  }

  @Test(expected = IllegalArgumentException.class)
  public void factoryRequireZooTest() {
    new ZkBackingStoreMgr.Factory().forAccumuloInstance("1234").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void factoryRequireInstanceTest() {
    ZooKeeper mockZooKeeper = EasyMock.createStrictMock(ZooKeeper.class);

    new ZkBackingStoreMgr.Factory().withZooKeeper(mockZooKeeper).build();
  }

  @Test
  public void getDefault() {

    // ZooKeeper zooMock = EasyMock.createStrictMock(ZooKeeper.class);
    // ZkPropCacheImpl propCache = EasyMock.createStrictMock(ZkPropCacheImpl.class);
    //
    // EasyMock.replay(zooMock, propCache);
    //
    // ZkBackingStoreMgr backingStore = new ZkBackingStoreMgr(zooMock);
    //
    // backingStore.load(TableId.of("abcd"));
    //
    // EasyMock.verify(zooMock, propCache);
  }

  @Test
  public void x() {
    // double pos = Math.ceil(Math.log(10) / Math.log(2));
    // log.info("P: {}", pos);

    int shift = (int) Math.ceil(Math.log(10) / Math.log(2));
    log.info("S: {}", (1 << shift) - 1);

    log.info("R: {}", Math.pow(2.0, Math.ceil(Math.log(10) / Math.log(2))));
    log.info("R: {}", Math.pow(2.0, Math.ceil(Math.log(100) / Math.log(2))));
    log.info("R: {}", Math.pow(2.0, Math.ceil(Math.log(255) / Math.log(2))));
    log.info("R: {}", Math.pow(2.0, Math.ceil(Math.log(511) / Math.log(2))));

    log.info("E: {}", mask(10, 1));
    log.info("E: {}", String.format("%02X", mask(10_000, 50)));

    log.info("B: {}", 31 - Integer.numberOfLeadingZeros(10));
    log.info("M1: {}", String.format("%02X", mask(5_000, 50)));
    log.info("M2: {}", String.format("%02X", mask2(5_000, 50)));

    log.info("M1: {}", String.format("%02X", mask(10_000, 50)));
    log.info("M2: {}", String.format("%02X", mask2(10_000, 50)));
    log.info("M2: {}", String.format("%02X", mask2(1_000, 25)));

    log.info("M2 of 7: {}", String.format("%02X", mask2(7, 1)));
    log.info("M2 of 8: {}", String.format("%02X", mask2(8, 1)));
  }

  private static int mask(final int expected, final int fanout) {
    int shift = (int) Math.ceil(Math.log(expected / fanout) / Math.log(2));
    return (1 << shift) - 1;
  }

  /**
   * Calculates a mask for bitwise AND that uses an expected number of nodes and a target fanout.
   * The mask the nearest power of 2 - 1 (equivalent to 2**(bits for expected / fanout) - 1. For
   * example, 1,000 nodes and a fanout of 25 would have a target of 40 nodes, the mask is 0x3f (63
   * base 10).
   *
   * @param expected
   *          the expected number of tables
   * @param fanout
   *          a target fanout for number of child nodes
   * @return the mask value
   */
  private static int mask2(final int expected, final int fanout) {
    int x = (int) Math.ceil(expected / (double) fanout);
    int shift = 31 - Integer.numberOfLeadingZeros(x);
    // log.info("X: {}, S: {}", x, shift);
    return (1 << (shift + 1)) - 1;
  }
}
