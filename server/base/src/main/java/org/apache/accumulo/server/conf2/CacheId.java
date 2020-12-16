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

import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.StringJoiner;

public class CacheId implements Comparable<CacheId> {

  private static final Logger log = LoggerFactory.getLogger(CacheId.class);

  final String iid;
  final AbstractId<?> id;

  public CacheId(final String instanceId, final AbstractId<?> id) {

    Objects.requireNonNull(instanceId, "Instance ID cannot be null");
    Objects.requireNonNull(id, "Id cannot be null");

    this.iid = instanceId;
    this.id = id;
  }

  public static CacheId fromKey(final String key) {
    Objects.requireNonNull(key, "Must provide CacheId string as uuid::type::id");

    // 0 iid, 1 type, 2 id
    String[] tokens = key.split("::");

    AbstractId<?> id;

    switch (IdType.valueOf(tokens[1])) {
      case NAMESPACE:
        id = NamespaceId.of(tokens[2]);
        return new CacheId(tokens[0], id);
      case TABLE:
        id = TableId.of(tokens[2]);
        return new CacheId(tokens[0], id);
      case DEFAULT:
      case SYSTEM:
      default:
        log.info("unhandled type: {}", tokens[2]);
    }
    return null;
  }

  public String getIID() {
    return iid;
  }

  public String getCanonicalId() {
    return id.canonical();
  }

  public String asKey() {
    return iid + "::" + getType().name() + "::" + id.canonical();
  }

  public IdType getType() {
    if (id instanceof TableId) {
      return IdType.TABLE;
    }
    if (id instanceof NamespaceId) {
      return IdType.NAMESPACE;
    }
    return IdType.DEFAULT;
  }

  @Override public String toString() {
    return new StringJoiner(", ", CacheId.class.getSimpleName() + "[", "]").add("iid='" + iid + "'")
        .add("id=" + id).toString();
  }

  @Override public int compareTo(CacheId other) {
    int r = iid.compareTo(other.iid);
    if (r != 0) {
      return r;
    }
    return id.canonical().compareTo(other.id.canonical());
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    CacheId cacheId = (CacheId) o;
    return Objects.equals(iid, cacheId.iid) && Objects.equals(id, cacheId.id);
  }

  @Override public int hashCode() {
    return Objects.hash(iid, id);
  }

  enum IdType {
    DEFAULT, SYSTEM, NAMESPACE, TABLE
  }
}
