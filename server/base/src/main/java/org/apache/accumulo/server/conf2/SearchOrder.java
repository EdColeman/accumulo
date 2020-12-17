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

import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.conf.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.LoadingCache;

public enum SearchOrder {
  TABLE {
    @Override
    SearchOrder search(final CacheId id, final String propName,
        final LoadingCache<CacheId,PropEncoding> cache) {
      log.trace("lookup table scope property - propName: {}", propName);
      String v = lookup(id, propName, cache);
      if (Objects.isNull(v)) {
        return NAMESPACE;
      }
      SearchOrder next = FOUND;
      next.propValue = v;
      return next;
    }
  },
  NAMESPACE {
    @Override
    SearchOrder search(final CacheId id, final String propName,
        final LoadingCache<CacheId,PropEncoding> cache) {
      log.trace("lookup namespace scope property {}", propName);
      CacheId nsid = new CacheId(id.getIID(), id.getNamespaceId(), null);
      String v = lookup(nsid, propName, cache);
      if (Objects.isNull(v)) {
        return SYSTEM;
      }
      SearchOrder next = FOUND;
      next.propValue = v;
      return next;
    }
  },
  SYSTEM {
    @Override
    SearchOrder search(final CacheId id, final String propName,
        final LoadingCache<CacheId,PropEncoding> cache) {
      log.trace("lookup system scope property {}", propName);
      String v = lookup(id, propName, cache);
      if (Objects.isNull(v)) {
        return DEFAULT;
      }
      SearchOrder next = FOUND;
      next.propValue = v;
      return next;
    }
  },
  DEFAULT {
    @Override
    SearchOrder search(final CacheId id, final String propName,
        final LoadingCache<CacheId,PropEncoding> cache) {
      log.trace("lookup default scope property {}", propName);
      Property p = Property.getPropertyByKey(propName);
      if (Objects.isNull(p)) {
        return NOT_PRESENT;
      }
      log.trace("found: {}={}", p, p.getDefaultValue());
      SearchOrder next = FOUND;
      next.propValue = p.getDefaultValue();
      return next;
    }
  },
  // terminal state.
  FOUND {
    @Override
    SearchOrder search(final CacheId id, final String propName,
        final LoadingCache<CacheId,PropEncoding> cache) {
      log.trace("property found");
      return FOUND;
    }
  },
  // terminal state.
  NOT_PRESENT {
    @Override
    SearchOrder search(final CacheId id, final String propName,
        final LoadingCache<CacheId,PropEncoding> cache) {
      log.trace("property not found, nowhere else to check");
      return NOT_PRESENT;
    }
  };

  private static final Logger log = LoggerFactory.getLogger(SearchOrder.class);

  abstract SearchOrder search(final CacheId id, final String propName,
      final LoadingCache<CacheId,PropEncoding> cache);

  private static String lookup(final CacheId id, final String propName,
      final LoadingCache<CacheId,PropEncoding> cache) {
    log.trace("get {} - {}", id, propName);
    String value = "";
    try {
      PropEncoding props = cache.get(id);
      String propValue = props.getProperty(propName);
      if (Objects.isNull(propValue)) {
        log.debug("search parents...");
        return null; // return searchParent("table", id, propName);
      }
      return propValue;
    } catch (ExecutionException ex) {
      log.error("failed", ex);
      throw new IllegalStateException(
          String.format("Failed to get property. id: %s, name: %s", id, propName));
    }
  }

  String propValue = "";
}
