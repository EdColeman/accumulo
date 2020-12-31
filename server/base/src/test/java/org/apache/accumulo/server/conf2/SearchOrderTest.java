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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.junit.Before;
import org.junit.Test;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class SearchOrderTest {

  private CacheId id1 = new CacheId(UUID.randomUUID().toString(), null, TableId.of("123"));
  private CacheId id2 =
      new CacheId(UUID.randomUUID().toString(), NamespaceId.of("321"), TableId.of("456"));

  private LoadingCache<CacheId,PropEncoding> cache;

  @Before
  public void init() {
    cache = CacheBuilder.newBuilder().build(new CacheLoader<>() {
      @Override
      public PropEncoding load(CacheId cacheId) throws Exception {
        PropEncoding props = new PropEncodingV1(1, true, Instant.now());
        props.addProperty("table.split.threshold", "1G");
        return props;
      }
    });

    PropEncoding props_123 = new PropEncodingV1(1, true, Instant.now());
    props_123.addProperty("table.split.threshold", "512M");

    cache.put(id1, props_123);

    PropEncoding props_321_456 = new PropEncodingV1(1, true, Instant.now());
    props_321_456.addProperty("table.split.endrow.size.max", "5K");
    cache.put(id2, props_321_456);
  }

  // validate search order
  @Test
  public void walkNotFound() {
    Optional<SearchOrder.LookupResult> result = SearchOrder.lookup(id1, "unknown", cache);
    assertTrue(result.isEmpty());
  }

  @Test
  public void findTableProp() {
    Optional<SearchOrder.LookupResult> result =
        SearchOrder.lookup(id1, "table.split.threshold", cache);
    assertTrue(result.isPresent());
    assertEquals("512M", result.get().getValue());
  }

  @Test
  public void findNamespace() {

    // not set on table 123, expect default
    Optional<SearchOrder.LookupResult> result =
        SearchOrder.lookup(id1, "table.split.endrow.size.max", cache);
    assertTrue(result.isPresent());
    assertEquals("10k", result.get().getValue());

    // namespace set on 456, expect override
    result = SearchOrder.lookup(id2, "table.split.endrow.size.max", cache);
    assertTrue(result.isPresent());
    assertEquals("5K", result.get().getValue());

  }

  @Test
  public void findSystem() {
    // TODO - needs implementation.
  }

  @Test
  public void findDefaultProp() {
    Optional<SearchOrder.LookupResult> result =
        SearchOrder.lookup(id1, "table.bloom.enabled", cache);
    assertTrue(result.isPresent());
    assertEquals("false", result.get().getValue());

  }

  @Test
  public void findDefaultProp2() {

    Optional<SearchOrder.LookupResult> result =
        SearchOrder.lookup(id1, "table.split.endrow.size.max", cache);

    assertTrue(result.isPresent());
    assertEquals("10k", result.get().getValue());
  }

}
