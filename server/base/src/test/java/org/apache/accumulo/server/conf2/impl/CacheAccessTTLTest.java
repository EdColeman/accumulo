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
package org.apache.accumulo.server.conf2.impl;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheAccessTTLTest {

  private static final Logger log = LoggerFactory.getLogger(CacheAccessTTLTest.class);

  @Test
  public void constructor() {
    ZooKeeper zooKeeper = EasyMock.mock(ZooKeeper.class);
    EasyMock.replay(zooKeeper);
    CacheAccessTTL cache = new CacheAccessTTL(zooKeeper);
    log.trace("cache: {}", cache);
  }

  @Test
  public void noNodeTest() throws InterruptedException, KeeperException {

    ZooKeeper zooKeeper = EasyMock.mock(ZooKeeper.class);
    Capture<Stat> stat = EasyMock.newCapture();

    EasyMock
        .expect(
            zooKeeper.getData(EasyMock.anyString(), EasyMock.anyBoolean(), EasyMock.capture(stat)))
        .andThrow(new KeeperException.NoNodeException() {});
    EasyMock.replay(zooKeeper);

    CacheId id = CacheId.forTable(UUID.randomUUID().toString(), TableId.of("a"));
    CacheAccessTTL cache = new CacheAccessTTL(zooKeeper);

    assertTrue(cache.load(id).isEmpty());

  }

  @Test
  public void simpleLoadTest() throws InterruptedException, KeeperException {

    CacheId id = CacheId.forTable(UUID.randomUUID().toString(), TableId.of("a"));

    PropEncoding props = new PropEncodingV1();

    ZooKeeper zooKeeper = EasyMock.mock(ZooKeeper.class);
    Capture<Stat> stat = EasyMock.newCapture();

    EasyMock
        .expect(
            zooKeeper.getData(EasyMock.anyString(), EasyMock.anyBoolean(), EasyMock.capture(stat)))
        .andAnswer(new IAnswer<byte[]>() {
          @Override
          public byte[] answer() throws Throwable {
            stat.getValue().setMzxid(1234);
            return props.toBytes();
          }
        });

    EasyMock.replay(zooKeeper);

    CacheAccessTTL cache = new CacheAccessTTL(zooKeeper);

    assertTrue(cache.load(id).isPresent());

  }

  @Test
  public void cleanerTest() throws InterruptedException, KeeperException {

    Instant testTime = Instant.parse("2021-05-01T12:34:56.000Z");
    Clock clock = Clock.fixed(testTime, ZoneId.of("UTC"));

    CacheId id = CacheId.forTable(UUID.randomUUID().toString(), TableId.of("a"));

    PropEncoding props = new PropEncodingV1();

    ZooKeeper zooKeeper = EasyMock.mock(ZooKeeper.class);
    Capture<Stat> stat = EasyMock.newCapture();

    EasyMock
        .expect(
            zooKeeper.getData(EasyMock.anyString(), EasyMock.anyBoolean(), EasyMock.capture(stat)))
        .andAnswer(new IAnswer<byte[]>() {
          @Override
          public byte[] answer() throws Throwable {
            stat.getValue().setMzxid(1234);
            return props.toBytes();
          }
        });

    CacheTTL cacheTTL = EasyMock.mock(CacheTTL.class);
    cacheTTL.update(anyObject(), anyObject());
    expectLastCall();

    EasyMock.replay(zooKeeper, cacheTTL);

    CacheAccessTTL cache = new CacheAccessTTL(zooKeeper, cacheTTL, clock);

    assertTrue(cache.load(id).isPresent());

  }
}
