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

import java.util.Objects;

import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheId implements Comparable<CacheId> {

  private static final Logger log = LoggerFactory.getLogger(CacheId.class);

  private final String iid;
  private final TableId tid;
  private final NamespaceId nid;

  public CacheId(final String instanceId, final String tableName) {
    this(instanceId, Tables.qualify(tableName));
  }

  public CacheId(final String instanceId, final Pair<String,String> pair) {
    this(instanceId, NamespaceId.of(pair.getFirst()), TableId.of(pair.getSecond()));
  }

  public CacheId(final String instanceId, final NamespaceId nid, final TableId tid) {
    Objects.requireNonNull(instanceId, "Instance ID cannot be null");
    this.iid = instanceId;
    this.nid = nid;
    this.tid = tid;
  }

  public static CacheId fromKey(final String key) {
    Objects.requireNonNull(key, "Must provide CacheId string as uuid::type::id");

    // 0 iid, 1 namespace id, 3 table id
    String[] tokens = key.split("::");

    return new CacheId(tokens[0], NamespaceId.of(tokens[1]), TableId.of(tokens[2]));
  }

  public String getIID() {
    return iid;
  }

  public TableId getTableId() {
    return tid;
  }

  public NamespaceId getNamespaceId() {
    return nid;
  }

  public String asKey() {
    return iid + "::" + nid.canonical() + "::" + tid.canonical();
  }

  public IdType getType() {

    if (!Objects.isNull(tid)) {
      return IdType.TABLE;
    }
    if (!Objects.isNull(nid)) {
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
  public int compareTo(CacheId o) {
    return 0;
  }

  enum IdType {
    DEFAULT, SYSTEM, NAMESPACE, TABLE
  }
}
