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
package org.apache.accumulo.server.conf2.util;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf2.CacheId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumpPropConfigTest {

  private static final Logger log = LoggerFactory.getLogger(DumpPropConfigTest.class);

  @Test
  public void x() {

    String id = UUID.randomUUID().toString();

    SortedSet<CacheId> sorted = new TreeSet<>(new CacheId.CacheIdComparator());

    sorted.add(CacheId.forSystem(id));
    sorted.add(CacheId.forNamespace(id, NamespaceId.of("+default")));
    sorted.add(CacheId.forNamespace(id, NamespaceId.of("+accumulo")));
    sorted.add(CacheId.forNamespace(id, NamespaceId.of("n1")));

    sorted.add(CacheId.forTable(id, TableId.of("t1")));
    sorted.add(CacheId.forTable(id, TableId.of("t2")));

    for (CacheId cid : sorted) {
      log.info("id: {} - {}", cid.asKey(), cid.path());
    }
  }

}
