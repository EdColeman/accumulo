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

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.PropStore;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * Simple wrapper around guava loading cache and a concrete cache implementation.
 */
public class CacheWrapper {

  private static final Logger log = LoggerFactory.getLogger(CacheWrapper.class);

  private static final long cacheTTL = 15;
  private static final TimeUnit cacheTTLUnits = TimeUnit.MINUTES;

  private final LoadingCache<CacheId,PropEncoding> cache;
  private final RemovalListener<CacheId,PropEncoding> removalListener = new RemovalListener<>() {
    @Override
    public void onRemoval(RemovalNotification<CacheId,PropEncoding> removalNotification) {
      log.debug("{} removed from cache", removalNotification.getKey());
    }
  };
  private PropStore store;

  public CacheWrapper(final PropStore store) {

    Objects.requireNonNull(store, "A PropStore implementation must be provided.");

    this.store = store;

    cache = CacheBuilder.newBuilder().expireAfterWrite(cacheTTL, cacheTTLUnits)
        .removalListener(removalListener).build(getLoader());
  }

  /**
   * TESTING - build cache with external time source so that tests do not need to wait for system
   * clock.
   *
   * @param ticker
   *          external time source.
   */
  public CacheWrapper(final PropStore store, final Ticker ticker) {
    Objects.requireNonNull(store, "A PropStore implementation must be provided.");

    this.store = store;

    cache = CacheBuilder.newBuilder().ticker(ticker).expireAfterWrite(cacheTTL, cacheTTLUnits)
        .removalListener(removalListener).build(getLoader());
  }

  private CacheLoader<CacheId,PropEncoding> getLoader() {

    CacheLoader<CacheId,PropEncoding> loader;
    loader = new CacheLoader<>() {
      @Override
      public PropEncoding load(final CacheId cacheId) throws Exception {
        Objects.requireNonNull(cacheId, "Invalid call to load cache with null cache id.");
        log.trace("Loading {} from backing store", cacheId);
        return store.readFromStore(cacheId);
      }
    };
    return loader;
  }

  public Optional<PropEncoding> getProperties(CacheId id) {
    try {
      var result = Optional.ofNullable(cache.get(id));
      if (log.isTraceEnabled()) {
        if (result.isPresent()) {
          log.trace("getProperties - from cache id {} - {}", id, result.get().print(true));
        } else {
          log.info("getProperties - from cache id {} is not present", id);
        }
      }
      return result;
    } catch (ExecutionException ex) {
      // TODO - evaluate if this should retry or throw checked exception so caller can.
      throw new IllegalStateException(ex);
    }
  }

  public void clear(CacheId id) {
    cache.invalidate(id);
  }

  public void clearAll() {
    cache.invalidateAll();
  }

  public CacheStats getStats() {
    cache.cleanUp();
    return cache.stats();
  }
}
