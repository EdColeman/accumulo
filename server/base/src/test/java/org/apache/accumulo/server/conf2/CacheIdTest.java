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

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheIdTest {

  private static final Logger log = LoggerFactory.getLogger(CacheIdTest.class);

  @Test
  public void typeTest() {
    CacheId id1 = new CacheId("a", null, TableId.of("table_a"));
    assertEquals(CacheId.IdType.TABLE, id1.getType());

    CacheId id2 = new CacheId("a", NamespaceId.of("123"), null);
    assertEquals(CacheId.IdType.NAMESPACE, id2.getType());
  }

  @Test
  public void keyTest() {
    UUID uuid = UUID.randomUUID();
    CacheId id1 = new CacheId(uuid.toString(), NamespaceId.of("321"), TableId.of("123"));

    log.debug("key: {}", id1.asKey());

    CacheId id2 = CacheId.fromKey(id1.asKey());

    assertEquals(id1, id2);
  }

  @Test
  public void parse() {
    CacheId id1 = new CacheId(UUID.randomUUID().toString(), NamespaceId.of("namespace_a"),
        TableId.of("table_a"));
    String uuid = id1.getIID();
    Pattern p = Pattern.compile("[0-9a-f\\-]{36}");
    Matcher m = p.matcher(uuid);
    assertTrue(m.matches());
  }

  @Test
  public void uuidTest() {
    CacheId id1 =
        new CacheId(UUID.randomUUID().toString(), NamespaceId.of("321"), TableId.of("123"));
    String uuid = id1.getIID();
    Pattern p = Pattern.compile("[0-9a-f\\-]{36}");
    Matcher m = p.matcher(uuid);
    assertTrue(m.matches());
  }
}
