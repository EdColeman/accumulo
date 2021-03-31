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

import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.Constants.ZROOT;

import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheId implements Comparable<CacheId> {

  public static final String NULL_ID = "-";
  public static final String SEPARATOR = "::";
  // TODO - move to Constants

  private static final Logger log = LoggerFactory.getLogger(CacheId.class);
  private static final Pattern pathPattern = Pattern.compile(ZROOT + "/(?<uuid>[a-f0-9-]{36})"
      + Constants.ZENCODED_CONFIG_ROOT + "/(?<ns>\\S+)::(?<tid>\\S+)");
  private final String iid;
  private final TableId tid;
  private final NamespaceId nid;

  public CacheId(final String instanceId, final NamespaceId nid, final TableId tid) {
    this.iid = requireNonNull(instanceId, "Instance ID cannot be null");
    validateInstanceId(instanceId);
    this.nid = nid;
    this.tid = tid;
  }

  private static void validateInstanceId(String uuid) throws IllegalArgumentException {
    try {
      UUID id = UUID.fromString(uuid);
    } catch (IllegalArgumentException ex) {
      log.warn("Invalid UUID '{}' provided", uuid);
      throw ex;
    }
  }

  /**
   * Returns the root path in zookeeper for encoded properties.
   *
   * @param instanceId
   *          the instance id
   * @return the property path in zookeeper
   */
  public static String getConfigRoot(final String instanceId) {
    return ZROOT + "/" + instanceId + Constants.ZENCODED_CONFIG_ROOT;
  }

  public static CacheId forSystem(final ServerContext context) {
    return forSystem(context.getInstanceID());
  }

  public static CacheId forSystem(final String instanceId) {
    return new CacheId(instanceId, null, null);
  }

  public static CacheId forNamespace(final ServerContext context, final NamespaceId namespaceId) {
    return forNamespace(context.getInstanceID(), namespaceId);
  }

  public static CacheId forNamespace(final String instanceId, final NamespaceId namespaceId) {
    return new CacheId(instanceId, namespaceId, null);
  }

  public static CacheId forTable(final ServerContext context, final TableId tableId) {
    return forTable(context.getInstanceID(), tableId);
  }

  public static CacheId forTable(final String instanceId, final TableId tableId) {
    return new CacheId(instanceId, null, tableId);
  }

  public static Optional<CacheId> fromPath(final String path) {
    Objects.requireNonNull(path, "path must be provided");

    Matcher matcher = pathPattern.matcher(path);

    if (matcher.matches()) {

      var iid = matcher.group("uuid");

      validateInstanceId(iid);

      var ns = parseNamespaceId(matcher.group("ns"));
      var tid = parseTableId(matcher.group("tid"));

      return Optional.of(new CacheId(iid, ns, tid));
    }

    return Optional.empty();
  }

  private static TableId parseTableId(String value) {
    TableId tid;
    if (NULL_ID.compareTo(value) == 0) {
      tid = null;
    } else {
      tid = TableId.of(value);
    }
    return tid;
  }

  private static NamespaceId parseNamespaceId(String value) {
    NamespaceId nid;
    if (NULL_ID.compareTo(value) == 0) {
      nid = null;
    } else {
      nid = NamespaceId.of(value);
    }
    return nid;
  }

  public String path() {
    return ZROOT + "/" + iid + Constants.ZENCODED_CONFIG_ROOT + "/" + nodeName();
  }

  public String nodeName() {
    return getNamespaceIdCanonical() + SEPARATOR + getTableIdCanonical();
  }

  public String getIID() {
    return iid;
  }

  public Optional<TableId> getTableId() {
    return Optional.ofNullable(tid);
  }

  private String getTableIdCanonical() {
    if (Objects.nonNull(tid)) {
      return tid.canonical();
    }
    return NULL_ID;
  }

  public Optional<NamespaceId> getNamespaceId() {
    return Optional.ofNullable(nid);
  }

  private String getNamespaceIdCanonical() {
    if (Objects.nonNull(nid)) {
      return nid.canonical();
    }
    return NULL_ID;
  }

  public String asKey() {
    return getNamespaceIdCanonical() + SEPARATOR + getTableIdCanonical();
  }

  public IdType getType() {

    if (Objects.nonNull(tid)) {
      return IdType.TABLE;
    }

    if (Objects.nonNull(nid)) {
      return IdType.NAMESPACE;
    }

    return IdType.SYSTEM;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("CacheId{ instance: ");
    sb.append(iid);
    if (Objects.isNull(tid) && Objects.isNull(nid)) {
      sb.append(", system");
    } else {
      if (Objects.nonNull(nid)) {
        sb.append(", nid: ");
        sb.append(nid.canonical());
      }
      if (Objects.nonNull(tid)) {
        sb.append(", tid: ");
        sb.append(tid.canonical());
      }
    }
    sb.append("}");

    return sb.toString();
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
    if (Objects.isNull(other)) {
      throw new IllegalArgumentException("Value to compare cannot be null");
    }
    return Comparator.comparing(CacheId::getIID).thenComparing(CacheId::getTableIdCanonical)
        .thenComparing(CacheId::getNamespaceIdCanonical).compare(this, other);
  }

  /**
   * Define types stored in zookeeper - defaults are not in zookeeper but come from code.
   */
  public enum IdType {
    UNKNOWN, SYSTEM, NAMESPACE, TABLE
  }

  /**
   * Sort by CacheId key with primary sort by system, namespace and then tables and then
   * alphabetically within each group.
   */
  public static class GroupByTypeComparator implements Comparator<CacheId> {
    Comparator<String> groupNames = new GroupByTypeNamesComparator();

    @Override
    public int compare(CacheId o1, CacheId o2) {
      return groupNames.compare(o1.asKey(), o2.asKey());
    }
  }

  public static class GroupByTypeNamesComparator implements Comparator<String> {
    @Override
    public int compare(String k1, String k2) {

      if (k1.startsWith("-::-") && k2.startsWith("-::-")) {
        return 0;
      }
      if (k1.startsWith("-::-")) {
        return -1;
      }
      if (k2.startsWith("-::-")) {
        return 1;
      }

      if (k1.startsWith("-::") && k2.startsWith("-::")) {
        // both are table ids
        return k1.compareTo(k2);
      }

      if (k1.startsWith("-::")) {
        return 1;
      }

      if (k2.startsWith("-::")) {
        return -1;
      }

      // both are namespace id
      return k1.compareTo(k2);
    }
  }
}
