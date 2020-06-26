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

import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.Pair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropValueTest {

  private static final Logger log = LoggerFactory.getLogger(PropValueTest.class);

  @Test
  public void compare() {
    Set<PropValue> aSet = new TreeSet<>();

    aSet.add(new PropValue.Builder().with($ -> {
      $.name = "abc";
      $.scope = PropScope.DEFAULT;
      $.value = "default";
    }).build());
    aSet.add(new PropValue.Builder().with($ -> {
      $.name = "abc";
      $.scope = PropScope.SITE;
      $.value = "site";
    }).build());
    aSet.add(new PropValue.Builder().with($ -> {
      $.name = "abc";
      $.scope = PropScope.SYSTEM;
      $.value = "system";
    }).build());
    aSet.add(new PropValue.Builder().with($ -> {
      $.name = "abc";
      $.scope = PropScope.NAMESPACE;
      $.value = "namespace";
    }).build());
    aSet.add(new PropValue.Builder().with($ -> {
      $.name = "abc";
      $.scope = PropScope.TABLE;
      $.value = "table";
    }).build());
    aSet.add(new PropValue.Builder().with($ -> {
      $.name = "zed";
      $.scope = PropScope.TABLE;
      $.value = "table";
    }).build());

    aSet.forEach((p) -> log.info("p:{}", p));

  }

  @Test
  public void namespaces() {

    Pair<String,String> p = Tables.qualify("bar", Namespace.DEFAULT.name());
    NamespaceId nid = NamespaceId.of(p.getFirst());
    TableId tid = TableId.of(p.getSecond());

    log.info("NID:{}, TID:{}", nid, tid);

    p = Tables.qualify("accumulo.bar", Namespace.DEFAULT.name());
    nid = NamespaceId.of(p.getFirst());
    tid = TableId.of(p.getSecond());

    log.info("NID:{}, TID:{}", nid, tid);

    log.info("q:{}", Tables.qualify("bar", Namespace.DEFAULT.name()));
    log.info("q:{}", Tables.qualify("foo.bar", Namespace.DEFAULT.name()));
    log.info("q:{}", Tables.qualify("accumulo.bar", Namespace.DEFAULT.name()));
  }
}
