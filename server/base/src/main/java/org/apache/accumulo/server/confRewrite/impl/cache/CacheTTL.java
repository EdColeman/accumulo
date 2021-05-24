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
package org.apache.accumulo.server.confRewrite.impl.cache;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.conf2.CacheId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks and provides cache item expiration to reduce cache size.
 */
class CacheTTL {

  public static final Duration TTL_DURATION = Duration.ofMillis(1000);
  private static final Logger log = LoggerFactory.getLogger(CacheTTL.class);
  private final BlockingDeque<AccessEntry> accessUpdateQueue = new LinkedBlockingDeque<>();
  private final Map<CacheId,Instant> accessMap = new HashMap<>();
  private final Lock mapLock = new ReentrantLock();

  private final ThreadPoolExecutor pool =
      ThreadPools.createFixedThreadPool(1, "ZooPropCleaner", false);

  private final AtomicBoolean running = new AtomicBoolean(true);
  private final Object cleanerMonitor = new Object();

  private final Clock clock;

  public CacheTTL() {
    this(Clock.systemUTC());
  }

  public CacheTTL(final Clock clock) {
    this.clock = clock;

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      running.set(false);
      // no reason to be nice, we are shutting down.
      pool.shutdownNow();
    }));

    pool.submit(this::updateTask);
  }

  private void updateTask() {
    log.info("update task started...");
    try {
      while (running.get()) {

        // grab all available when take becomes available. The map
        // de-duplicates entries, keeping the last access time.
        Map<CacheId,Instant> entries = new HashMap<>();

        var entry = accessUpdateQueue.take();
        log.info("Process entry from queue: {}", entry);
        entries.put(entry.id, entry.accessTime);
        AccessEntry e;
        while (Objects.nonNull(e = accessUpdateQueue.poll())) {
          entries.put(e.id, e.accessTime);
        }

        log.info("Set contains {} elements: {}", entries.size(), entries);

        updateMap(entries);

        Thread.yield();
      }
    } catch (InterruptedException ex) {
      // this will propagate the interrupt if anyone checks, but task will keep running.
      Thread.currentThread().interrupt();
    }
  }

  public void clearAll() {
    try {
      mapLock.lockInterruptibly();

      accessUpdateQueue.clear();
      accessMap.clear();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Locking ttl cache interrupted", ex);
    } finally {
      mapLock.unlock();
    }
  }

  public void clear(CacheId cacheId) {
    try {
      mapLock.lockInterruptibly();
      accessMap.remove(cacheId);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Locking ttl cache interrupted", ex);
    } finally {
      mapLock.unlock();
    }
  }

  public List<CacheId> getExpired(final Instant timestamp) {
    List<CacheId> expired = new ArrayList<>();
    try {
      mapLock.lock();

      // accessMap.entrySet().stream()
      // .filter(x -> (Duration.between(x.getValue(), timestamp).compareTo(TTL_DURATION) > 0))
      // .map(x -> x.getKey()).collect(Collectors.toList());

      accessMap.forEach((k, v) -> {
        if (Duration.between(v, timestamp).compareTo(TTL_DURATION) > 0) {
          expired.add(k);
          accessMap.remove(k);
        }
      });

      return expired;

    } finally {
      mapLock.unlock();
    }
  }

  private void updateMap(Map<CacheId,Instant> entries) {
    try {
      mapLock.lock();
      accessMap.putAll(entries);
    } finally {
      mapLock.unlock();
    }
  }

  public void update(CacheId cacheId, Instant timestamp) {

    log.info("add entry on update - id {}", cacheId);

    var success = accessUpdateQueue.offer(new AccessEntry(cacheId, timestamp));
    if (!success) {
      log.debug(
          "Failed to update cache access time - cache entries may expire sooner than expected");
    }
  }

  private static class AccessEntry implements Comparable<AccessEntry> {
    private final CacheId id;
    private final Instant accessTime;

    public AccessEntry(final CacheId id, final Instant accessTime) {
      this.id = id;
      this.accessTime = accessTime;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      AccessEntry that = (AccessEntry) o;
      return id.equals(that.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }

    @Override
    public int compareTo(AccessEntry other) {
      return id.compareTo(other.id);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", AccessEntry.class.getSimpleName() + "[", "]").add("id=" + id)
          .add("accessTime=" + accessTime).toString();
    }
  }
}
