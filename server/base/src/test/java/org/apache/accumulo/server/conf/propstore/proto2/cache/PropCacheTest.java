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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf.propstore.dummy.ZkOps;
import org.apache.accumulo.server.conf.propstore.dummy.ZkOpsImpl;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;

import edu.umd.cs.findbugs.annotations.NonNull;

public class PropCacheTest {

  private static final Logger log = LoggerFactory.getLogger(PropCacheTest.class);

  private final String rootPath = "/accumulo/1234/config";
  private final TableId tableId = TableId.of("abc1");

  @Test
  public void loadEmpty() throws Exception {

    ZooKeeper zooMock = EasyMock.createStrictMock(ZooKeeper.class);

    // call to get data without a node - throw NoNodeException
    EasyMock
        .expect(zooMock.getData(EasyMock.isA(String.class), EasyMock.anyBoolean(),
            EasyMock.isA(Stat.class)))
        .andThrow(new KeeperException.NoNodeException("path does not exist"));

    EasyMock.replay(zooMock);

    ZkOps zoo = new ZkOpsImpl(zooMock);

    PropCache cache = new PropCacheImpl(rootPath, zoo);

    PropMap m = cache.get(tableId);

    // the "empty" map should be in the cache so call to zookeeper is not expected.
    PropMap n = cache.get(tableId);

    assertSame("same instance expected", m, n);

    log.trace("M:{} ", m);

    EasyMock.verify(zooMock);
  }

  @Test
  public void loadWithData() throws Exception {

    var data = PropMapZooLoaderTest.genData(tableId);

    ZooKeeper zooMock = EasyMock.createStrictMock(ZooKeeper.class);

    // call to get data without a node - throw NoNodeException
    EasyMock.expect(zooMock.getData(EasyMock.isA(String.class), EasyMock.anyBoolean(),
        EasyMock.isA(Stat.class))).andAnswer(() -> {
          Stat s = (Stat) EasyMock.getCurrentArguments()[2];
          s.setPzxid(123);
          s.setVersion(1);
          s.setDataLength(data.length);
          return data;
        });

    EasyMock.replay(zooMock);

    ZkOps zoo = new ZkOpsImpl(zooMock);

    PropCache cache = new PropCacheImpl(rootPath, zoo);

    PropMap m = cache.get(tableId);

    log.info("M:{} ", m);

    EasyMock.verify(zooMock);
  }

  @Test
  public void reloadOnUpdate() throws Exception {

    ZooKeeper zooMock = EasyMock.createStrictMock(ZooKeeper.class);

    // call to get data without a node - throw NoNodeException
    EasyMock
        .expect(zooMock.getData(EasyMock.isA(String.class), EasyMock.anyBoolean(),
            EasyMock.isA(Stat.class)))
        .andThrow(new KeeperException.NoNodeException("path does not exist")).times(2);

    EasyMock.replay(zooMock);

    ZkOps zoo = new ZkOpsImpl(zooMock);

    PropCache cache = new PropCacheImpl(rootPath, zoo);

    PropMap m = cache.get(tableId);

    // invalidate the cache to simulate update, should result in second zookeeper call.
    cache.invalidate(tableId);
    PropMap n = cache.get(tableId);

    assertNotSame("different instance expected", m, n);

    log.info("M:{} ", m);

    EasyMock.verify(zooMock);
  }

  /**
   * Direct test of LoadingCache functionality.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void timeExpire() throws Exception {

    CacheLoader<String,String> loader;
    loader = new CacheLoader<>() {
      @Override
      public String load(@NonNull String s) {
        return s.toUpperCase();
      }
    };
    RemovalListener<String,String> removalListener = removalNotification -> log
        .info("removed {} - {}", removalNotification.getKey(), removalNotification.getCause());

    LoadingCache<String,String> cache =
        CacheBuilder.newBuilder().expireAfterAccess(30, TimeUnit.MILLISECONDS)
            .removalListener(removalListener).build(loader);

    cache.getUnchecked("abc");
    assertEquals(1, cache.size());

    log.info("s: {}", cache.stats());

    Thread.sleep(5000);
    cache.cleanUp();
    cache.getUnchecked("xyz");

    log.info("v: {}", cache.getIfPresent("abc"));
    log.info("s: {}", cache.stats());

    assertEquals(1, cache.size());

  }
}
