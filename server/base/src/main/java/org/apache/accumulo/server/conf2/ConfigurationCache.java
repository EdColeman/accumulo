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

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.*;

public class ConfigurationCache implements Configuration {

  private static final Logger log = LoggerFactory.getLogger(ConfigurationCache.class);

  private final PropStore backingStore;

  private final LoadingCache<CacheId,PropEncoding> cache;

  private final PropertyChangeListener notifier;

  public ConfigurationCache(final PropStore backingStore) {
    Objects.requireNonNull(backingStore, "A backing store must be provided");

    this.backingStore = backingStore;

    cache = CacheBuilder.newBuilder().removalListener(removalListener).build(getLoader());

    notifier = new Notifier(cache);
    backingStore.registerForChanges(notifier);

  }

  RemovalListener<CacheId,PropEncoding> removalListener =
      new RemovalListener<CacheId,PropEncoding>() {
        @Override
        public void onRemoval(RemovalNotification<CacheId,PropEncoding> removalNotification) {
          log.debug("{} removed from cache", removalNotification.getKey());
        }
      };

  private CacheLoader<CacheId,PropEncoding> getLoader() {
    CacheLoader<CacheId,PropEncoding> loader;
    loader = new CacheLoader<CacheId,PropEncoding>() {
      @Override
      public PropEncoding load(CacheId cacheId) throws Exception {
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
      return result.get().getValue();
    }

    return "";
  }

  private String searchParent(final String level, final CacheId id, final String propName) {
    return "default";
  }

  @Override
  public void setProperty(final CacheId id, final String propName, final String value) {

  }

  public PropertyChangeListener getNotifier() {
    return notifier;
  }

  private static class Notifier implements PropertyChangeListener {
    private final LoadingCache<CacheId,PropEncoding> backingCache;

    public Notifier(final LoadingCache<CacheId,PropEncoding> cache) {
      this.backingCache = cache;
    }

    @Override
    public void propertyChange(PropertyChangeEvent pce) {
      log.debug("Received prop change event {}", pce);
      CacheId id = CacheId.fromKey(pce.getPropertyName());
      log.warn("HAVE key: {}, {}", id, backingCache.asMap().containsKey(id));
      backingCache.invalidate(id);
    }
  }
}
