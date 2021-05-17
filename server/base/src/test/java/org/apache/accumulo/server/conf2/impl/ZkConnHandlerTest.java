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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkConnHandlerTest {

  private static final Logger log = LoggerFactory.getLogger(ZkConnHandlerTest.class);

  private final int numPoolThreads = 5;
  private final int numWorkerThreads = 4;

  private CountDownLatch startLatch = null;
  private CountDownLatch readyLatch = null;

  private ThreadPoolExecutor pool = null;

  @Before
  public void init() {
    startLatch = new CountDownLatch(numWorkerThreads);
    readyLatch = new CountDownLatch(numWorkerThreads);

    pool = ThreadPools.createFixedThreadPool(numPoolThreads, "zk-conn-test-pool", false);
  }

  @After
  public void teardown() {
    pool.shutdownNow();
    try {
      pool.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      // don't care.
      pool.shutdownNow();
    }
  }

  @Test
  public void connectedTest() throws InterruptedException {

    ZooBackedCache cache = EasyMock.mock(ZooBackedCache.class);
    cache.clearAll();
    EasyMock.expectLastCall();

    EasyMock.replay(cache);

    ZkConnHandler handler = new ZkConnHandler(cache);

    WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.None,
        Watcher.Event.KeeperState.SyncConnected, "/a/path");

    handler.process(event);

    handler.blockUntilReady();
  }

  @Test
  public void disconnectTest() throws InterruptedException {
    ZooBackedCache cache = EasyMock.mock(ZooBackedCache.class);
    cache.clearAll();
    EasyMock.expectLastCall();

    EasyMock.replay(cache);

    ZkConnHandler handler = new ZkConnHandler(cache);
    WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.None,
        Watcher.Event.KeeperState.SyncConnected, "/a/path");

    handler.process(event);

    assertTrue(handler.isZkConnected());

    handler.blockUntilReady();

    WatchedEvent event2 = new WatchedEvent(Watcher.Event.EventType.None,
        Watcher.Event.KeeperState.Disconnected, "/a/path");

    handler.process(event2);

    assertFalse(handler.isZkConnected());

    startLatch = new CountDownLatch(1);
    readyLatch = new CountDownLatch(1);

    ReadyTask r = new ReadyTask(handler, startLatch, readyLatch);
    Future<Long> blockedTask = pool.submit(r);

    Thread.sleep(1000);
    assertFalse(blockedTask.isDone());

  }

  /**
   * Runs multiple threads. The call to resync the cache shculd only be called once.
   *
   * @throws InterruptedException
   *           any exception is a test failure
   */
  @Test
  public void blockingTest() throws InterruptedException {

    ZooBackedCache cache = EasyMock.mock(ZooBackedCache.class);
    cache.clearAll();
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(cache);

    ZkConnHandler handler = new ZkConnHandler(cache);

    List<Future<Long>> tasks = new ArrayList<>();
    for (int i = 0; i < numWorkerThreads; i++) {
      ReadyTask r = new ReadyTask(handler, startLatch, readyLatch);
      tasks.add(pool.submit(r));
    }

    log.info("Wait for tasks to start");

    var haveExpected = startLatch.await(13_000, TimeUnit.MILLISECONDS);
    assertTrue(haveExpected);

    log.info("Tasks ready");

    log.info("Sending connected event");
    handler.process(new WatchedEvent(Watcher.Event.EventType.None,
        Watcher.Event.KeeperState.SyncConnected, "/a/path"));

    log.info("Connected event sent");

    tasks.forEach(f -> {
      try {
        var p = f.get();
        log.info("Received: {}", p);
      } catch (ExecutionException | InterruptedException ex) {
        log.info("Task failed", ex);
      }
    });

    EasyMock.verify(cache);
  }

  private static class ReadyTask implements Callable<Long> {
    private final ZkConnHandler handler;
    private final CountDownLatch startLatch;
    private final CountDownLatch completeLatch;

    public ReadyTask(final ZkConnHandler handler, final CountDownLatch startLatch,
        final CountDownLatch completeLatch) {
      this.handler = handler;
      this.startLatch = startLatch;
      this.completeLatch = completeLatch;
    }

    @Override
    public Long call() throws Exception {
      long start = System.currentTimeMillis();
      log.info("hit 1");
      startLatch.countDown();
      // startLatch.await();
      log.info("block 1");
      handler.blockUntilReady();
      log.info("block returned");
      completeLatch.countDown();
      return System.currentTimeMillis() - start;
    }
  }
}
