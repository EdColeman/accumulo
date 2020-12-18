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
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class ConfigurationCache implements Configuration {

  private static final Logger log = LoggerFactory.getLogger(ConfigurationCache.class);

  private final PropStore backingStore;

  private final LoadingCache<CacheId,PropEncoding> cache;

  private final PropertyChangeListener notifier = new Notifier();

  public ConfigurationCache(final PropStore backingStore) {
    this.backingStore = backingStore;
    cache = CacheBuilder.newBuilder().build(getLoader());
  }

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

  private static class Notifier implements PropertyChangeListener {
    @Override
    public void propertyChange(PropertyChangeEvent pce) {
      log.debug("Received prop change event", pce);
    }
  }
}
