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

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationCacheTest {

  private static final Logger log = LoggerFactory.getLogger(ConfigurationCacheTest.class);

  private CacheId iid = null;
  private ConfigurationCache cache = null;

  // seed test "backend" store with test props
  private PropStore store;

  @Before
  public void setup() {

    store = new MemPropStore();

    // make a fake instance id
    var uuid = UUID.randomUUID().toString();

    PropEncoding t123Props = new PropEncodingV1(1, true, Instant.now());
    t123Props.addProperty("table.split.threshold", "512M");
    t123Props.addProperty("table.file.max", "5");

    iid = new CacheId(uuid, NamespaceId.of("321"), TableId.of("123"));
    store.set(iid, t123Props);

    PropEncoding ns1 = new PropEncodingV1(1, true, Instant.now());
    ns1.addProperty("table.split.endrow.size.max", "5k");

    CacheId iid2 = new CacheId(uuid, NamespaceId.of("321"), null);
    store.set(iid2, ns1);

    cache = new ConfigurationCache(store);
  }

  /**
   * not set - should return default.
   */
  @Test
  public void defaultPropTest() {

    // not set - should return default.
    String enabled = cache.getProperty(iid, "table.bloom.enabled");
    assertEquals("false", enabled);

    // cache.getProperty(iid, "table.split.threshold");

  }

  /**
   * should return table override.
   */
  @Test
  public void tablePropTest() {

    // not set - should return default.
    String splitThreshold = cache.getProperty(iid, "table.split.threshold");
    assertEquals("512M", splitThreshold);

  }

  /**
   * should return namespace override.
   */
  @Test
  public void namespacePropTest() {

    // not set - should return default.
    String splitThreshold = cache.getProperty(iid, "table.split.endrow.size.max");
    assertEquals("5k", splitThreshold);

  }

  @Test
  public void changeNotificationTest() {

    String bloomSizeDefault = cache.getProperty(iid, "table.bloom.size");

    PropEncoding props = store.get(iid, cache.getNotifier());
    props.addProperty("table.bloom.size", "1024");
    store.set(iid, props);

    String bloomSize = cache.getProperty(iid, "table.bloom.size");

    props.addProperty("table.bloom.size", "2048");
    store.set(iid, props);

    String bloomSize2 = cache.getProperty(iid, "table.bloom.size");

    log.debug("Prop default {}, first: {}, second: {}", bloomSizeDefault, bloomSize, bloomSize2);

  }
}
