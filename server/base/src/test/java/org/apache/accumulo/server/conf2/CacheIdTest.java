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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheIdTest {

  private static final Logger log = LoggerFactory.getLogger(CacheIdTest.class);

  @Test
  public void typeTest() {

    var tid = "321";
    var ns = "123";

    CacheId id1 = new CacheId(UUID.randomUUID().toString(), null, TableId.of(tid));
    assertEquals(CacheId.IdType.TABLE, id1.getType());
    assertEquals(tid, id1.getTableId().get().canonical());

    CacheId id2 = new CacheId(UUID.randomUUID().toString(), NamespaceId.of(ns), null);
    assertEquals(CacheId.IdType.NAMESPACE, id2.getType());
    assertEquals(ns, id2.getNamespaceId().get().canonical());

    CacheId id3 = new CacheId(UUID.randomUUID().toString(), null, null);
    assertEquals(CacheId.IdType.SYSTEM, id3.getType());
    assertTrue(id3.getNamespaceId().isEmpty());
    assertTrue(id3.getTableId().isEmpty());

    // fail on invalid uuid in constructor
    try {
      new CacheId("0", NamespaceId.of(ns), null);
      fail("Expected IllegalArgumentException on bad UUID");
    } catch (IllegalArgumentException ex) {
      // empty - exception is required with invalid uuid
    }
  }

  @Test
  public void compareTest() {
    UUID uuid = UUID.randomUUID();
    CacheId id1 = new CacheId(uuid.toString(), NamespaceId.of("321"), TableId.of("123"));

    var path = id1.path();
    CacheId id2 = CacheId.fromPath(path).orElse(
        new CacheId("00000000-0000-0000-0000-000000000000", NamespaceId.of("*"), TableId.of("*")));

    assertEquals(0, id1.compareTo(id2));

    assertEquals(id1.hashCode(), Objects.requireNonNull(id2).hashCode());
  }

  @Test
  public void keyTest() {
    UUID uuid = UUID.randomUUID();
    CacheId id1 = new CacheId(uuid.toString(), NamespaceId.of("321"), TableId.of("123"));

    log.debug("path: {}", id1.path());

    CacheId id2 = CacheId.fromPath(id1.path()).orElse(
        new CacheId("00000000-0000-0000-0000-000000000000", NamespaceId.of("*"), TableId.of("*")));

    assertEquals(id1, id2);
  }

  @Test
  public void invalidPath1Test() {

    // go path
    var goPath =
        "/accumulo/c227b233-39b9-47b6-a7da-7a1035941ac0" + Constants.ZENCODED_CONFIG_ROOT + "/1::1";
    Optional<CacheId> id1 = CacheId.fromPath(goPath);
    assertTrue("path failed to pass validation", id1.isPresent());

    var badPath =
        "/error/c227b233-39b9-47b6-a7da-7a1035941ac0" + Constants.ZENCODED_CONFIG_ROOT + "/1::1";
    Optional<CacheId> id2 = CacheId.fromPath(badPath);
    log.trace("should reject invalid path: {} - received {}", badPath, id2);
    assertFalse("path should have failed to pass validation - does not start with /accumulo",
        id2.isPresent());
  }

  // bad uuid (length)
  @Test
  public void invalidPath2Test() {
    var badPath =
        "/accumulo/1-39b9-47b6-a7da-7a1035941ac0" + Constants.ZENCODED_CONFIG_ROOT + "/1::1";
    Optional<CacheId> id = CacheId.fromPath(badPath);
    log.trace("should reject invalid path: {} - received {}", badPath, id);
    assertFalse("path should have failed to pass validation - does not have valid uuid",
        id.isPresent());
  }

  // bad uuid - invalid char
  @Test
  public void invalidPath3Test() {

    var path =
        "/accumulo/Z227b233-39b9-47b6-a7da-7a1035941ac0" + Constants.ZENCODED_CONFIG_ROOT + "/1::1";
    Optional<CacheId> id = CacheId.fromPath(path);
    log.trace("should reject invalid path: {} - received {}", path, id);
    assertFalse("path should have failed to pass validation - invalid char in uuid",
        id.isPresent());
  }

  // bad uuid not configuration path
  @Test
  public void invalidPath4Test() {

    var path = "/accumulo/c227b233-39b9-47b6-a7da-7a1035941ac0/error/1::1";
    Optional<CacheId> id = CacheId.fromPath(path);
    log.trace("should reject invalid path: {} - received {}", path, id);
    assertFalse("path should have failed to pass validation - invalid configuration path",
        id.isPresent());
  }

  // bad uuid - missing ns
  @Test
  public void invalidPath5Test() {

    var path =
        "/accumulo/c227b233-39b9-47b6-a7da-7a1035941ac0" + Constants.ZENCODED_CONFIG_ROOT + "/::1";
    Optional<CacheId> id = CacheId.fromPath(path);
    log.trace("should reject invalid path: {} - received {}", path, id);
    assertFalse("path should have failed to pass validation - invalid node name", id.isPresent());
  }

  // bad uuid - missing tid
  @Test
  public void invalidPath6Test() {

    var path =
        "/accumulo/c227b233-39b9-47b6-a7da-7a1035941ac0" + Constants.ZENCODED_CONFIG_ROOT + "/1::";
    Optional<CacheId> id = CacheId.fromPath(path);
    log.trace("should reject invalid path: {} - received {}", path, id);
    assertFalse("path should have failed to pass validation - invalid node name", id.isPresent());
  }

  // bad uuid - invalid ns::tid separator
  @Test
  public void invalidPath7Test() {

    var path =
        "/accumulo/c227b233-39b9-47b6-a7da-7a1035941ac0" + Constants.ZENCODED_CONFIG_ROOT + "/1-1";
    Optional<CacheId> id = CacheId.fromPath(path);
    log.trace("should reject invalid path: {} - received {}", path, id);
    assertFalse("path should have failed to pass validation - invalid node name", id.isPresent());
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

  @Test
  public void pathTest() {
    CacheId id1 = new CacheId(UUID.randomUUID().toString(), NamespaceId.of("namespace_a"),
        TableId.of("table_a"));
    log.debug("path: {}", id1.path());
    log.debug("key: {}", id1.asKey());

    log.info("ID1: {}", CacheId.fromPath(id1.path()));

    CacheId id2 = new CacheId(UUID.randomUUID().toString(), null, null);
    CacheId.fromPath(id2.path());

    log.info("ID2: {}", CacheId.fromPath(id2.path()));

  }
}
