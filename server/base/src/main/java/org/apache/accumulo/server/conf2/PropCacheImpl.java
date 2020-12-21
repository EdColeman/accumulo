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
package org.apache.accumulo.server.conf2;

import java.beans.PropertyChangeListener;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class PropCacheImpl implements PropCache {

  private static final Logger log = LoggerFactory.getLogger(PropCacheImpl.class);

  private final PropStore backingStore;

  private final LoadingCache<CacheId,PropEncoding> cache;

  private final Notifier notifier;

  public PropCacheImpl(final PropStore backingStore) {
    Objects.requireNonNull(backingStore, "A backing store must be provided");

    this.backingStore = backingStore;

    cache = CacheBuilder.newBuilder().removalListener(removalListener).build(getLoader());

    notifier = new Notifier(cache);
    backingStore.registerForChanges(notifier);

  }

  RemovalListener<CacheId,PropEncoding> removalListener = new RemovalListener<>() {
    @Override
    public void onRemoval(RemovalNotification<CacheId,PropEncoding> removalNotification) {
      log.debug("{} removed from cache", removalNotification.getKey());
      notifier.onRemoval(removalNotification);
    }
  };

  private CacheLoader<CacheId,PropEncoding> getLoader() {
    CacheLoader<CacheId,PropEncoding> loader;
    loader = new CacheLoader<>() {
      @Override
      public PropEncoding load(final CacheId cacheId) throws Exception {
        Objects.requireNonNull(cacheId, "Invalid call to load cache with null cache id.");
        log.trace("Loading {} from backing store", cacheId);
        return backingStore.get(cacheId, notifier);
      }
    };
    return loader;
  }

  @Override
  public String getProperty(final CacheId iid, final String propName) {
    log.trace("get {} - {}", iid, propName);

    Optional<SearchOrder.LookupResult> result = SearchOrder.lookup(iid, propName, cache);

    if (result.isPresent()) {
      SearchOrder.LookupResult lookupResult = result.get();
      notifier.monitor(lookupResult.getKey());
      return lookupResult.getValue();
    }

    return "";
  }

  @Override
  public void setProperty(final CacheId id, final String propName, final String value) {

  }

  public CacheStats getStats() {
    cache.cleanUp();
    return cache.stats();
  }

  public PropertyChangeListener getNotifier() {
    return notifier;
  }

}
