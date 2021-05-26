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
package org.apache.accumulo.server.confRewrite.cache;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.confRewrite.zk.BackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class PropGuavaCache implements PropCache {

  private static final Logger log = LoggerFactory.getLogger(PropGuavaCache.class);

  private static final long cacheTTL = 15;
  private static final TimeUnit cacheTTLUnits = TimeUnit.MINUTES;

  private final BackingStore zooProps;
  private final LoadingCache<CacheId,PropEncoding> cache;

  public PropGuavaCache(final BackingStore zooProps) {
    this.zooProps = zooProps;
    cache = CacheBuilder.newBuilder().expireAfterWrite(cacheTTL, cacheTTLUnits).build(getLoader());
  }

  private CacheLoader<CacheId,PropEncoding> getLoader() {

    CacheLoader<CacheId,PropEncoding> loader;
    loader = new CacheLoader<>() {
      @Override
      public PropEncoding load(final CacheId cacheId) throws Exception {
        Objects.requireNonNull(cacheId, "Invalid call to load cache with null cache id.");
        log.trace("Loading {} from backing store", cacheId);

        PropEncoding props = zooProps.readFromStore(cacheId);
        if (null == props) {
          throw new NoSuchPropsException(cacheId);
        }
        return props;
      }
    };
    return loader;
  }

  @Override
  public PropEncoding getProperties(CacheId id) {
    try {
      PropEncoding p = cache.get(id);
      log.info("Cached value:{} - {}", id, p.print(true));
      return p;
    } catch (ExecutionException ex) {
      if (ex.getCause() instanceof NoSuchPropsException) {
        log.info("No properties for {}", id);
        return null;
      }
      log.info("WHAT?");
      throw new IllegalStateException("Failed to read " + id + " from guava cache", ex);
    }
  }

  @Override
  public void clear(final CacheId id) {
    cache.invalidate(id);
  }

  @Override
  public void clearAll() {
    cache.invalidateAll();
  }

  private static class NoSuchPropsException extends Exception {
    private static final long serialVersionUID = 1;

    private CacheId id;

    public NoSuchPropsException(final CacheId id) {
      this("Zookeeper node for " + id + " not found");
      this.id = id;
    }

    public NoSuchPropsException(final String message) {
      super(message);
    }
  }

}
