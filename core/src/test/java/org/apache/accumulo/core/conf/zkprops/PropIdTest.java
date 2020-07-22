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
package org.apache.accumulo.core.conf.zkprops;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.data.TableId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropIdTest {

  private static final Logger log = LoggerFactory.getLogger(PropIdTest.class);

  @Test
  public void simple() {

    PropId id = new PropId.Builder().with($ -> {
      $.propName = "abc";
      $.scope = PropId.Scope.TABLE;
      $.id = "foo.bar";
    }).build();

    log.debug("PropId: {}", id);

    assertTrue(id.hasNamespace());
    assertEquals("foo", id.getNamespaceId().get().canonical());

    assertTrue(id.hasTableId());
    assertEquals("bar", id.getTableId().get().canonical());

  }

  /**
   * Validate that setting Scope.NAMESPACE and the id parses correctly
   */
  @Test
  public void namespaceScope() {

    PropId id = new PropId.Builder().with($ -> {
      $.propName = "abc";
      $.scope = PropId.Scope.NAMESPACE;
      $.id = "foo";
    }).build();

    log.debug("PropId: {}", id);

    assertTrue(id.hasNamespace());
    assertEquals("foo", id.getNamespaceId().get().canonical());

    assertFalse(id.hasTableId());
    assertTrue(id.getTableId().isEmpty());

  }

  /**
   * Validate that setting Scope.NAMESPACE and the id parses correctly
   */
  @Test
  public void tableScope() {

    PropId id = new PropId.Builder().with($ -> {
      $.propName = "abc";
      $.scope = PropId.Scope.TABLE;
      $.id = "foo";
    }).build();

    log.debug("PropId: {}", id);

    assertFalse(id.hasNamespace());
    assertTrue("", id.getNamespaceId().isEmpty());

    assertTrue(id.hasTableId());
    assertEquals("foo", id.getTableId().get().canonical());

  }

  @Test
  public void sortOrder() {

    Set<PropId> sorter = new TreeSet<>();

    sorter.add(new PropId.Builder().with($ -> {
      $.propName = "zzz";
      $.scope = PropId.Scope.TABLE;
      $.id = "foo.bar";
    }).build());

    sorter.add(new PropId.Builder().with($ -> {
      $.propName = "abc";
      $.scope = PropId.Scope.DEFAULT;
    }).build());

    sorter.add(new PropId.Builder().with($ -> {
      $.propName = "abc";
      $.scope = PropId.Scope.SITE;
    }).build());

    sorter.add(new PropId.Builder().with($ -> {
      $.propName = "abc";
      $.scope = PropId.Scope.SYSTEM;
    }).build());

    sorter.add(new PropId.Builder().with($ -> {
      $.propName = "abc";
      $.scope = PropId.Scope.NAMESPACE;
      $.id = "foo";
    }).build());

    sorter.add(new PropId.Builder().with($ -> {
      $.propName = "abc";
      $.scope = PropId.Scope.TABLE;
      $.id = "foo.bar";
    }).build());

    sorter.forEach(id -> log.trace("{}", id));

    // the expected order should match precedence of scope.
    Iterator<PropId> ids = sorter.iterator();
    for (PropId.Scope s : PropId.Scope.values()) {
      assertEquals(ids.next().scope, s);
    }

    // last entry
    PropId last = ids.next();

    assertEquals(last.propName, "zzz");
    assertEquals(last.scope, PropId.Scope.TABLE);
    last.namespaceId.ifPresentOrElse(n -> assertEquals(n.canonical(), "foo"),
        () -> fail("expected namespace to be present"));
    last.tableId.ifPresentOrElse(t -> assertEquals(t.canonical(), "bar"),
        () -> fail("expected tableId to be present"));

  }

  @Test
  public void x() {
    TableId tableId = null;

    Optional<TableId> tiopt = Optional.ofNullable(tableId).filter(t -> !t.canonical().isEmpty());

    log.info("Null: {}", tiopt);
  }
}
