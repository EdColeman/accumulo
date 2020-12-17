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

import java.time.Instant;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class SearchOrderTest {

  private CacheId iid;
  private LoadingCache<CacheId,PropEncoding> cache;

  @Before
  public void init() {
    iid = new CacheId(UUID.randomUUID().toString(), "123");
    cache = CacheBuilder.newBuilder().build(new CacheLoader<>() {
      @Override
      public PropEncoding load(CacheId cacheId) throws Exception {
        PropEncoding props = new PropEncodingV1(1, true, Instant.now());
        props.addProperty("table.split.threshold", "512M");
        return props;
      }
    });
  }

  // validate search order
  @Test
  public void walkNotFound() {

    SearchOrder search = SearchOrder.TABLE;

    SearchOrder next = search.search(iid, "invalid", cache);
    assertEquals(SearchOrder.NAMESPACE, next);

    next = next.search(iid, "invalid", cache);
    assertEquals(SearchOrder.SYSTEM, next);

    next = next.search(iid, "invalid", cache);
    assertEquals(SearchOrder.DEFAULT, next);

    next = next.search(iid, "invalid", cache);
    assertEquals(SearchOrder.NOT_PRESENT, next);

  }

  @Test
  public void findTableProp() {
    SearchOrder search = SearchOrder.TABLE;

    SearchOrder next = search.search(iid, "table.split.threshold", cache);
    assertEquals(SearchOrder.FOUND, next);
    assertEquals("512M", next.propValue);
  }

  @Test
  public void findDefaultProp() {
    SearchOrder next = SearchOrder.TABLE;

    while (next != SearchOrder.FOUND && next != SearchOrder.NOT_PRESENT) {
      next = next.search(iid, "table.bloom.enabled", cache);
    }

    assertEquals(SearchOrder.FOUND, next);
    assertEquals("false", next.propValue);
  }

  @Test
  public void findDefaultProp2() {
    SearchOrder next = SearchOrder.TABLE;

    while (next != SearchOrder.FOUND && next != SearchOrder.NOT_PRESENT) {
      next = next.search(iid, "table.split.endrow.size.max", cache);
    }

    assertEquals(SearchOrder.FOUND, next);
    assertEquals("10k", next.propValue);
  }
}
