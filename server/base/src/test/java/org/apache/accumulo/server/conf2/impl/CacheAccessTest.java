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
package org.apache.accumulo.server.conf2.impl;

import java.time.Instant;
import java.util.UUID;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf2.CacheId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheAccessTest {

  private static final Logger log = LoggerFactory.getLogger(CacheAccessTest.class);

  @Test
  public void addEntryTest() throws Exception {

    CacheAccess access = new CacheAccess();
    CacheId id = CacheId.forTable(UUID.randomUUID().toString(), TableId.of("a"));
    Thread.sleep(4_000);

    Instant now = Instant.now();
    log.info("Now: {}", now);
    access.update(id, Instant.ofEpochMilli(now.toEpochMilli()));
    access.update(id, Instant.ofEpochMilli(now.toEpochMilli() + 10_000));
    access.update(id, Instant.ofEpochMilli(now.toEpochMilli() + 20_000));
    access.update(id, Instant.ofEpochMilli(now.toEpochMilli() + 30_000));

    Thread.sleep(4_000);
    access.update(id, Instant.ofEpochMilli(now.toEpochMilli() + 40_000));
    Thread.sleep(4_000);
  }
}
