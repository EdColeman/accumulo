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
import static org.apache.accumulo.core.Constants.ZNAMESPACE_CONF;
import static org.apache.accumulo.core.Constants.ZROOT;
import static org.apache.accumulo.core.Constants.ZTABLES;
import static org.apache.accumulo.core.Constants.ZTABLE_CONF;

import java.util.Comparator;
import java.util.Objects;
import java.util.StringJoiner;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropCacheId2 implements Comparable<PropCacheId2> {

  private static final Logger log = LoggerFactory.getLogger(PropCacheId2.class);
  public static final String PROP_NODE_NAME = "encoded_props";

  private final String path;

  private PropCacheId2(final String path) {
    this.path = path;
  }

  public static PropCacheId2 forSystem(final ServerContext context) {
    return forSystem(context.getInstanceID());
  }

  public static PropCacheId2 forSystem(final String instanceId) {
    return new PropCacheId2(ZROOT + "/" + instanceId + ZCONFIG + "/" + PROP_NODE_NAME);
  }

  public static PropCacheId2 forNamespace(final ServerContext context,
      final NamespaceId namespaceId) {
    return forNamespace(context.getInstanceID(), namespaceId);
  }

  public static PropCacheId2 forNamespace(final String instanceId, final NamespaceId namespaceId) {
    return new PropCacheId2(ZROOT + "/" + instanceId + ZNAMESPACES + "/" + namespaceId.canonical()
        + ZNAMESPACE_CONF + "/" + PROP_NODE_NAME);
  }

  public static PropCacheId2 forTable(final ServerContext context, final TableId tableId) {
    return forTable(context.getInstanceID(), tableId);
  }

  public static PropCacheId2 forTable(final String instanceId, final TableId tableId) {
    return new PropCacheId2(ZROOT + "/" + instanceId + ZTABLES + "/" + tableId.canonical()
        + ZTABLE_CONF + "/" + PROP_NODE_NAME);
  }

  public String getPath() {
    return path;
  }

  @Override
  public int compareTo(PropCacheId2 other) {
    // return path.compareTo(other.getPath());
    return Comparator.comparing(PropCacheId2::getIdType).thenComparing(PropCacheId2::getPath)
        .compare(this, other);
  }

  /**
   * Define types stored in zookeeper - defaults are not in zookeeper but come from code.
   */
  public enum IdType {
    UNKNOWN, SYSTEM, NAMESPACE, TABLE
  }

  public IdType getIdType() {
    String[] tokens = path.split("/");
    if (tokens.length == 0 || !tokens[tokens.length - 1].equals(PROP_NODE_NAME)) {
      return IdType.UNKNOWN;
    }
    if (tokens[3].equals(ZTABLES.substring(1))) {
      return IdType.TABLE;
    }
    if (tokens[3].equals(ZNAMESPACES.substring(1))) {
      return IdType.NAMESPACE;
    }
    return IdType.SYSTEM;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    PropCacheId2 that = (PropCacheId2) o;
    return path.equals(that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", PropCacheId2.class.getSimpleName() + "[", "]")
        .add("path='" + path + "'").toString();
  }
}
