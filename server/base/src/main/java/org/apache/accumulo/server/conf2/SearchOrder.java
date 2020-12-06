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
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.LoadingCache;

public class SearchOrder {

  private static final Logger log = LoggerFactory.getLogger(SearchOrder.class);

  public static class LookupResult {
    private final String value;
    private final CacheId key;

    public LookupResult(final String value, final CacheId key) {
      this.value = value;
      this.key = key;
    }

    public String getValue() {
      return value;
    }

    public CacheId getKey() {
      return key;
    }

    @Override
    public String toString() {
      return "LookupResult{" + "value='" + value + '\'' + ", key=" + key + '}';
    }
  }

  public static Optional<LookupResult> lookup(final CacheId id, final String propName,
      final LoadingCache<CacheId,PropEncoding> cache) {

    Objects.requireNonNull(id, "Must provide a CacheId for lookup");

    try {
      Optional<LookupResult> value;

      // lookup by table
      log.trace("lookup table scoped property {} {}", id, propName);
      value = getLookupResult(id, propName, cache);
      if (value.isPresent())
        return value;

      // lookup using namespace
      if (id.getNamespaceId().isPresent()) {
        NamespaceId nsid = id.getNamespaceId().get();
        CacheId id2 = new CacheId(id.getIID(), nsid, null);
        log.trace("lookup namespace scoped property {} {}", nsid, propName);
        value = getLookupResult(id2, propName, cache);
        if (value.isPresent())
          return value;
      }

      // lookup system
      // TODO - load and check system configuration.
      log.trace("lookup system scoped property {} {}", id.getNamespaceId(), propName);

      // look up default
      log.trace("lookup default scope property {}", propName);
      Property p = Property.getPropertyByKey(propName);
      if (Objects.isNull(p)) {
        return Optional.empty();
      }

      return Optional.of(new LookupResult(p.getDefaultValue(), null));

    } catch (ExecutionException ex) {
      throw new IllegalStateException("Failed to lookup " + id.toString(), ex);
    }
  }

  private static Optional<LookupResult> getLookupResult(final CacheId id, final String propName,
      final LoadingCache<CacheId,PropEncoding> cache) throws ExecutionException {

    PropEncoding props = cache.get(id);
    if (Objects.nonNull(props)) {
      String value = props.getProperty(propName);
      if (Objects.nonNull(value)) {
        return Optional.of(new LookupResult(value, id));
      }
    }
    return Optional.empty();
  }
}
