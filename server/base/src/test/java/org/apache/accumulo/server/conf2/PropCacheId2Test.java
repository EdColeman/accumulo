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

import static org.apache.accumulo.core.Constants.ZCONFIG;
import static org.apache.accumulo.core.Constants.ZNAMESPACES;
import static org.apache.accumulo.core.Constants.ZTABLES;
import static org.apache.accumulo.server.conf2.PropCacheId2.PROP_NODE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropCacheId2Test {
  private static final Logger log = LoggerFactory.getLogger(PropCacheId2Test.class);

  private final String instanceId = UUID.randomUUID().toString();

  @Test
  public void systemType() {
    var name = PropCacheId2.forSystem(instanceId);
    log.info("name: {}", name);
    assertTrue(name.getPath().endsWith(ZCONFIG + "/" + PROP_NODE_NAME));
    assertEquals(PropCacheId2.IdType.SYSTEM, name.getIdType());

  }

  @Test
  public void namespaceType() {
    var name = PropCacheId2.forNamespace(instanceId, NamespaceId.of("a"));
    log.info("name: {}", name);
    assertTrue(name.getPath().endsWith(PROP_NODE_NAME) && name.getPath().contains(ZNAMESPACES));
    assertEquals(PropCacheId2.IdType.NAMESPACE, name.getIdType());
    log.info("name: {}", name);
  }

  @Test
  public void tableType() {
    var name = PropCacheId2.forTable(instanceId, TableId.of("a"));
    log.info("name: {}", name);
    assertTrue(name.getPath().endsWith(PROP_NODE_NAME) && name.getPath().contains(ZTABLES));
    assertEquals(PropCacheId2.IdType.TABLE, name.getIdType());
    log.info("name: {}", name);
  }

  @Test
  public void sortTest() {
    Set<PropCacheId2> nodes = new TreeSet<>();
    nodes.add(PropCacheId2.forTable(instanceId, TableId.of("z1")));
    nodes.add(PropCacheId2.forTable(instanceId, TableId.of("a1")));
    nodes.add(PropCacheId2.forTable(instanceId, TableId.of("x1")));
    nodes.add(PropCacheId2.forNamespace(instanceId, NamespaceId.of("z2")));
    nodes.add(PropCacheId2.forNamespace(instanceId, NamespaceId.of("a2")));
    nodes.add(PropCacheId2.forNamespace(instanceId, NamespaceId.of("x2")));

    nodes.add(PropCacheId2.forSystem(instanceId));

    Iterator<PropCacheId2> iterator = nodes.iterator();
    assertEquals(PropCacheId2.IdType.SYSTEM, iterator.next().getIdType());
    assertEquals(PropCacheId2.IdType.NAMESPACE, iterator.next().getIdType());
    assertEquals(PropCacheId2.IdType.NAMESPACE, iterator.next().getIdType());
    assertEquals(PropCacheId2.IdType.NAMESPACE, iterator.next().getIdType());
    assertEquals(PropCacheId2.IdType.TABLE, iterator.next().getIdType());
    assertEquals(PropCacheId2.IdType.TABLE, iterator.next().getIdType());
    assertEquals(PropCacheId2.IdType.TABLE, iterator.next().getIdType());

    // rewind.
    iterator = nodes.iterator();
    iterator.next(); // skip system
    assertTrue(iterator.next().getPath().contains("a2"));
    assertTrue(iterator.next().getPath().contains("x2"));
    assertTrue(iterator.next().getPath().contains("z2"));

    assertTrue(iterator.next().getPath().contains("a1"));
    assertTrue(iterator.next().getPath().contains("x1"));
    assertTrue(iterator.next().getPath().contains("z1"));

    log.info("Sorted: {}", nodes);
  }
}
