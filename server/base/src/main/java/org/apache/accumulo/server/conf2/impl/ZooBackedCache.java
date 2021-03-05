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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooBackedCache {

  private static final Logger log = LoggerFactory.getLogger(ZooBackedCache.class);

  private final Map<CacheId,PropZNode> cache = new HashMap<>();
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock rLock = rwLock.readLock();
  private final ReentrantReadWriteLock.WriteLock wLock = rwLock.writeLock();

  private final ZooKeeper zooKeeper;
  private final Clock clock;

  private final CacheTTL cacheTTL;
  private final ScheduledExecutorService scheduler =
      ThreadPools.createScheduledExecutorService(1, "ZooPropCleaner", false);

  private final Metrics metrics = new Metrics();

  private final ZkConnHandler zkConnHandler;

  public ZooBackedCache(final ZooKeeper zooKeeper) {
    this(zooKeeper, new CacheTTL(), Clock.systemUTC());
  }

  public ZooBackedCache(final ZooKeeper zooKeeper, final CacheTTL cacheTTL, final Clock clock) {
    this.zooKeeper = zooKeeper;
    this.cacheTTL = cacheTTL;
    this.clock = clock;

    this.zkConnHandler = new ZkConnHandler(this);

    scheduler.scheduleAtFixedRate(new CacheExpireTask(this, cacheTTL, clock), 1_000, 3_000,
        TimeUnit.SECONDS);

    Runtime.getRuntime()
        .addShutdownHook(new Thread(() -> new ShutdownTask(zkConnHandler, scheduler)));

  }

  public Optional<PropEncoding> load(final CacheId id) throws InterruptedException {

    Instant accessStart = clock.instant();

    zkConnHandler.blockUntilReady();

    try {
      metrics.updateAccessCount();
      try {
        PropZNode p = getPropZNode(id, accessStart);
        if (null != p) {
          metrics.updateHitCount();
          return Optional.of(p.getProps());
        }
      } catch (KeeperException.NoNodeException ex) {
        return Optional.empty();
      } catch (KeeperException ex) {
        // todo - evaluate handling
        throw new IllegalStateException("Zookeeper read interrupted", ex);
      }

      // not found in local cache - read from zookeeper.
      try {
        wLock.lock();

        // read again under write lock - another thread could have loaded it while we were waiting.
        PropZNode zNode = cache.get(id);
        if (Objects.nonNull(zNode)) {
          return Optional.of(zNode.getProps());
        }

        Instant zkAccessStart = clock.instant();
        metrics.updateZkLoadCount();
        try {
          // not found - get from zookeeper.
          Stat stat = new Stat();
          byte[] data = zooKeeper.getData(id.path(), false, stat);
          PropEncodingV1 props = new PropEncodingV1(data);
          zNode = new PropZNode(props, stat, clock.instant());

          log.info("Loaded: {}", zNode);

          cache.put(id, zNode);

          return Optional.of(props);
        } finally {
          metrics.updateZkLoadTime(Duration.between(zkAccessStart, clock.instant()).toNanos());
        }
      } catch (KeeperException.NoNodeException ex) {
        return Optional.empty();
      } catch (KeeperException ex) {
        log.warn("Loading properties from zookeeper for {} failed", id, ex);
        throw new IllegalStateException("Loading properties from zookeeper for " + id + " failed",
            ex);
      } finally {
        wLock.unlock();
      }

    } finally {
      cacheTTL.update(id, accessStart);
      metrics.updateAccessTime(Duration.between(accessStart, clock.instant()).toNanos());
    }
  }

  void clearAll() throws InterruptedException {
    wLock.lockInterruptibly();
    try {
      cache.clear();
    } finally {
      wLock.unlock();
    }
    cacheTTL.clearAll();
  }

  public Map<String,Long> getMetrics() {
    return metrics.snapshot();
  }

  /**
   * Performs thread safe get access to cache using a read lock.
   *
   * @param id
   *          the cache id
   * @return the properties if found in cache.
   */
  private PropZNode getPropZNode(final CacheId id, final Instant timestamp)
      throws InterruptedException, KeeperException {

    PropZNode zNode;
    try {
      rLock.lockInterruptibly();
      zNode = cache.get(id);
    } finally {
      rLock.unlock();
    }

    if (null != zNode) {

      // validate based on last update
      if (Duration.between(zNode.getLastCheck(), timestamp).compareTo(Duration.ofMillis(1000))
          > 0) {

        log.info("SYNC CHECK expired");

        Stat current = zooKeeper.exists(id.path(), false);
        if (current.getMzxid() != zNode.getMzxid()) {
          // node has changed since saved.
          try {
            wLock.lockInterruptibly();
            cache.remove(id);
          } finally {
            wLock.unlock();
          }
          return null;
        }

        // stat modified time still valid - update access time and write to cache.
        try {

          PropZNode update = new PropZNode(zNode.props, current, timestamp);
          wLock.lockInterruptibly();
          cache.put(id, update);
        } finally {
          wLock.unlock();
        }
      }
      return zNode;
    }

    return null;
  }

  private static class ShutdownTask implements Runnable {

    private final ScheduledExecutorService scheduler;
    private final ZkConnHandler zkConnHandler;

    public ShutdownTask(final ZkConnHandler zkConnHandler,
        final ScheduledExecutorService scheduler) {
      this.zkConnHandler = zkConnHandler;
      this.scheduler = scheduler;
    }

    @Override
    public void run() {
      try {
        zkConnHandler.disable();
        scheduler.shutdownNow();
      } catch (Exception ex) {
        // empty
      }
    }
  }

  private static class CacheExpireTask implements Runnable {

    private final ZooBackedCache zooBackedCache;
    private final CacheTTL cacheTTL;
    private final Clock clock;

    public CacheExpireTask(final ZooBackedCache zooBackedCache, final CacheTTL cacheTTL,
        final Clock clock) {
      this.zooBackedCache = zooBackedCache;
      this.cacheTTL = cacheTTL;
      this.clock = clock;
    }

    @Override
    public void run() {
      log.info("Clean...");
      List<CacheId> expired = cacheTTL.getExpired(clock.instant());
      try {
        zooBackedCache.wLock.lock();
        expired.forEach(id -> {
          var removedId = zooBackedCache.cache.remove(id);
          if (removedId != null) {
            log.trace("Expired: {}", id);
          }
        });
      } finally {
        zooBackedCache.wLock.unlock();
      }
    }
  }

  static class Metrics {

    private final AtomicLong accessCount = new AtomicLong();
    private final AtomicLong accessTime = new AtomicLong();
    private final AtomicLong zkLoadCount = new AtomicLong();
    private final AtomicLong zkLoadTime = new AtomicLong();
    private final AtomicLong hitCount = new AtomicLong();

    public void updateAccessCount() {
      accessCount.incrementAndGet();
    }

    public void updateAccessTime(final long deltaNanos) {
      accessTime.addAndGet(deltaNanos);
    }

    public void updateZkLoadCount() {
      zkLoadCount.incrementAndGet();
    }

    public void updateZkLoadTime(final long deltaNanos) {
      zkLoadTime.addAndGet(deltaNanos);
    }

    public void updateHitCount() {
      hitCount.incrementAndGet();
    }

    public Map<String,Long> snapshot() {

      Map<String,Long> snapshot = new HashMap<>();

      snapshot.put("accessCount", accessCount.get());
      snapshot.put("accessTime", accessTime.get());
      snapshot.put("zkLoadCount", zkLoadCount.get());
      snapshot.put("zkLoadTime", zkLoadTime.get());
      snapshot.put("hitCount", hitCount.get());

      return snapshot;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", Metrics.class.getSimpleName() + "[", "]")
          .add("accessCount=" + accessCount.get()).add("accessTime=" + accessTime.get())
          .add("zkLoadCount=" + zkLoadCount.get()).add("zkLoadTime=" + zkLoadTime.get())
          .add("hitCount=" + hitCount.get()).toString();
    }
  }

  private static class PropZNode {

    private final PropEncoding props;
    private final long mzxid;
    private final Instant lastCheck;

    public PropZNode(final PropEncoding props, final Stat stat, final Instant lastCheck) {
      this.props = props;
      this.mzxid = stat.getMzxid();
      this.lastCheck = lastCheck;
    }

    public PropEncoding getProps() {
      return props;
    }

    public long getMzxid() {
      return mzxid;
    }

    public Instant getLastCheck() {
      return lastCheck;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", PropZNode.class.getSimpleName() + "[", "]")
          .add("props=" + props).add("mzxid=" + mzxid).add("lastCheck=" + lastCheck).toString();
    }
  }
}
