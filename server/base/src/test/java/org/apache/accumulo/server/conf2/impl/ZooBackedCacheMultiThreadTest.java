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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooBackedCacheMultiThreadTest {

  private static final Logger log = LoggerFactory.getLogger(ZooBackedCacheMultiThreadTest.class);

  /**
   * This test verifies that the load call to zookeeper is executed once. All threads should return
   * a value, but only one should get the write lock and proceed to the zookeeper call. This is
   * validated by the mock expecting a single zookeeper call.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void multiThreadReadOneLoad() throws Exception {

    ZooKeeper zooKeeper = EasyMock.mock(ZooKeeper.class);

    PropEncoding props = new PropEncodingV1();
    Capture<Stat> stat = EasyMock.newCapture();

    EasyMock
        .expect(
            zooKeeper.getData(EasyMock.anyString(), EasyMock.anyBoolean(), EasyMock.capture(stat)))
        .andAnswer(() -> {
          stat.getValue().setMzxid(1234);
          return props.toBytes();
        });

    EasyMock.replay(zooKeeper);
    ZooBackedCache cache = new ZooBackedCache(zooKeeper);

    var numPoolThreads = 5;
    var numWorkerThreads = 7;
    CountDownLatch startLatch = new CountDownLatch(numWorkerThreads);
    CountDownLatch readyLatch = new CountDownLatch(numPoolThreads);

    ThreadPoolExecutor pool =
        ThreadPools.createFixedThreadPool(numPoolThreads, "reader-pool", false);

    CacheId id = CacheId.forTable(UUID.randomUUID().toString(), TableId.of("a"));

    List<Future<Optional<PropEncoding>>> tasks = new ArrayList<>();
    for (int i = 0; i < numWorkerThreads; i++) {
      ReaderTask r = new ReaderTask(cache, id, startLatch, readyLatch);
      tasks.add(pool.submit(r));
    }

    readyLatch.await();

    tasks.forEach(f -> {
      try {
        var p = f.get();
        log.info("Received: {}", p);
      } catch (ExecutionException | InterruptedException ex) {
        log.info("Task failed", ex);
      }
    });

    Map<String,Long> metrics = cache.getMetrics();
    log.info("metrics: {}", metrics);
  }

  private static class ReaderTask implements Callable<Optional<PropEncoding>> {

    private final ZooBackedCache cache;
    private final CacheId id;
    private final CountDownLatch startLatch;
    private final CountDownLatch completeLatch;

    public ReaderTask(final ZooBackedCache cache, final CacheId id, final CountDownLatch startLatch,
        final CountDownLatch completeLatch) {
      this.cache = cache;
      this.id = id;
      this.startLatch = startLatch;
      this.completeLatch = completeLatch;
    }

    @Override
    public Optional<PropEncoding> call() throws Exception {
      try {
        startLatch.countDown();

        log.info("Start wait");
        // pause instead of block to allow for more threads than in pool.
        var hitZero = startLatch.await(200, TimeUnit.MILLISECONDS);
        log.info("Wait complete - timed out {}", !hitZero);
        return cache.load(id);

      } finally {
        completeLatch.countDown();
      }
    }
  }
}
