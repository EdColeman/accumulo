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

import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;

import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheId implements Comparable<CacheId> {

  private static final Logger log = LoggerFactory.getLogger(CacheId.class);
  public static final String NULL_ID = "-";
  public static final String SEPERATOR = "::";

  private final String iid;
  private final Optional<TableId> tid;
  private final Optional<NamespaceId> nid;

  public CacheId(final String instanceId, final String tableName) {
    this(instanceId, Tables.qualify(tableName));
  }

  public CacheId(final String instanceId, final Pair<String,String> pair) {
    this(instanceId, NamespaceId.of(pair.getFirst()), TableId.of(pair.getSecond()));
  }

  public CacheId(final String instanceId, final NamespaceId nid, final TableId tid) {
    Objects.requireNonNull(instanceId, "Instance ID cannot be null");
    this.iid = instanceId;
    this.nid = Optional.ofNullable(nid);
    this.tid = Optional.ofNullable(tid);
  }

  public static CacheId fromKey(final String key) {
    Objects.requireNonNull(key, "Must provide CacheId string as uuid::type::id");

    // 0 iid, 1 namespace id, 3 table id
    String[] tokens = key.split(SEPERATOR);

    NamespaceId nid;
    if (NULL_ID.compareTo(tokens[1]) == 0) {
      nid = null;
    } else {
      nid = NamespaceId.of(tokens[1]);
    }

    TableId tid;
    if (NULL_ID.compareTo(tokens[2]) == 0) {
      tid = null;
    } else {
      tid = TableId.of(tokens[2]);
    }

    return new CacheId(tokens[0], nid, tid);
  }

  public String getIID() {
    return iid;
  }

  public Optional<TableId> getTableId() {
    return tid;
  }

  private String getTableIdCanonical() {
    if (tid.isPresent()) {
      return tid.get().canonical();
    }
    return NULL_ID;
  }

  public Optional<NamespaceId> getNamespaceId() {
    return nid;
  }

  private String getNamespaceIdCanonical() {
    if (nid.isPresent()) {
      return nid.get().canonical();
    }
    return NULL_ID;
  }

  public String asKey() {
    return iid + SEPERATOR + getNamespaceIdCanonical() + SEPERATOR + getTableIdCanonical();
  }

  public IdType getType() {

    if (tid.isPresent()) {
      return IdType.TABLE;
    }
    if (nid.isPresent()) {
      return IdType.NAMESPACE;
    }

    return IdType.DEFAULT;
  }

  @Override
  public String toString() {
    return "CacheId{" + "iid='" + iid + '\'' + ", tid=" + tid + ", nid=" + nid + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    CacheId cacheId = (CacheId) o;
    return Objects.equals(iid, cacheId.iid) && Objects.equals(tid, cacheId.tid)
        && Objects.equals(nid, cacheId.nid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(iid, tid, nid);
  }

  @Override
  public int compareTo(CacheId other) {
    return Comparator.comparing(CacheId::getIID).thenComparing(CacheId::getTableIdCanonical)
        .thenComparing(CacheId::getNamespaceIdCanonical).compare(this, other);
  }

  enum IdType {
    DEFAULT, SYSTEM, NAMESPACE, TABLE
  }
}
