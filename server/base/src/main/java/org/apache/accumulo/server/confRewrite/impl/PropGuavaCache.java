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
package org.apache.accumulo.server.confRewrite.impl;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.confRewrite.PropCache;
import org.apache.accumulo.server.confRewrite.zk.ZkPropStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class PropGuavaCache implements PropCache {

  private static final Logger log = LoggerFactory.getLogger(PropGuavaCache.class);

  private static final long cacheTTL = 15;
  private static final TimeUnit cacheTTLUnits = TimeUnit.MINUTES;

  private final ZkPropStore zooProps;
  private final LoadingCache<CacheId,Optional<PropEncoding>> cache;

  public PropGuavaCache(final ZkPropStore zooProps) {
    this.zooProps = zooProps;
    cache = CacheBuilder.newBuilder().expireAfterWrite(cacheTTL, cacheTTLUnits).build(getLoader());
  }

  private CacheLoader<CacheId,Optional<PropEncoding>> getLoader() {

    CacheLoader<CacheId,Optional<PropEncoding>> loader;
    loader = new CacheLoader<>() {
      @Override
      public Optional<PropEncoding> load(final CacheId cacheId) throws Exception {
        Objects.requireNonNull(cacheId, "Invalid call to load cache with null cache id.");
        log.trace("Loading {} from backing store", cacheId);
        return Optional.ofNullable(zooProps.readFromStore(cacheId));
      }
    };
    return loader;
  }

  @Override
  public Optional<PropEncoding> getProperties(CacheId id) {
    try {
      return cache.get(id);
    } catch (ExecutionException ex) {
      throw new IllegalStateException("Failed to read " + id + " from guava cache", ex);
    }
  }

  @Override
  public void clear(CacheId id) {
    cache.invalidate(id);
  }

  @Override
  public void clearAll() {
    cache.invalidateAll();
  }

}
