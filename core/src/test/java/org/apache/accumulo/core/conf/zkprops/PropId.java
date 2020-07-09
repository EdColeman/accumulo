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

import static java.util.Comparator.comparing;

import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.Pair;

/**
 * Properties are identified with the following components:
 * <p/>
 * <ul>
 * <li>prop name</li>
 * <li>scope</li>
 * <li>namespace id (optional)</li>
 * <li>table id (optional)</li>
 * </ul>
 * <p/>
 * Properties are hierarchical, ordered from general to specific scope. When accessing a property
 * the most specific value is returned, unless all scopes are requested.
 */
public class PropId implements Comparable<PropId> {

  final String propName;
  final Scope scope;
  final Optional<NamespaceId> namespaceId;
  final Optional<TableId> tableId;

  public PropId(String propName, Scope scope, TableId tableId, NamespaceId namespaceId) {
    this.propName = propName;
    this.scope = scope;
    this.tableId = Optional.ofNullable(tableId).filter(t -> !t.canonical().isEmpty());
    this.namespaceId = Optional.ofNullable(namespaceId).filter(n -> !n.canonical().isEmpty());
  }

  @SuppressWarnings("unused") // used by gson
  private PropId() {
    propName = "";
    scope = Scope.DEFAULT;
    tableId = Optional.empty();
    namespaceId = Optional.empty();
  }

  /**
   * Compares an optional, with nulls sorting last.
   *
   * @param <T>
   *          a Optional, Comparable instance.
   */
  private static class OptionalComparator<T extends Comparable<T>>
      implements Comparator<Optional<T>> {
    @Override
    public int compare(Optional<T> obj1, Optional<T> obj2) {
      if (obj1.isPresent() && obj2.isPresent()) {
        return obj1.get().compareTo(obj2.get());
      } else if (obj1.isPresent()) {
        return 1;
      } else if (obj2.isPresent()) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  private static Comparator<PropId> getComparator() {
    OptionalComparator<TableId> tableIdComparator = new OptionalComparator<>();
    OptionalComparator<NamespaceId> namespaceIdComparator = new OptionalComparator<>();

    Comparator<PropId> result = comparing(PropId::getPropName).thenComparing(PropId::getScope)
        .thenComparing(n -> n.namespaceId, namespaceIdComparator)
        .thenComparing(t -> t.tableId, tableIdComparator);

    return result;
  }

  public String getPropName() {
    return propName;
  }

  public Scope getScope() {
    return scope;
  }

  public boolean hasNamespace() {
    return namespaceId.isPresent();
  }

  public Optional<NamespaceId> getNamespaceId() {
    return namespaceId;
  }

  public boolean hasTableId() {
    return tableId.isPresent();
  }

  public Optional<TableId> getTableId() {
    return tableId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    PropId propId = (PropId) o;
    return Objects.equals(propName, propId.propName) && scope == propId.scope
        && Objects.equals(tableId, propId.tableId)
        && Objects.equals(namespaceId, propId.namespaceId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(propName, scope, tableId, namespaceId);
  }

  @Override
  public String toString() {
    return "PropId{" + "propName='" + propName + '\'' + ", scope=" + scope + ", tableId=" + tableId
        + ", namespaceId=" + namespaceId + '}';
  }

  /**
   * Compares two PropIds - sort order defined buy (1) name, (2) scope, (3) namespace, (4) table id
   *
   * @param other
   *          instance of PropId to compare
   * @return an negative integer, zero or a positive integer if less than, equal to, or greater.
   */
  @Override
  public int compareTo(PropId other) {
    return getComparator().compare(this, other);
  }

  /**
   * Properties are hierarchical, ordered from general to specific scope. When accessing a property
   * the most specific value is returned.
   */
  public enum Scope {
    DEFAULT, SITE, SYSTEM, NAMESPACE, TABLE
  }

  public static class Builder {

    String propName;
    Scope scope = Scope.DEFAULT;
    String id;

    public PropId.Builder with(Consumer<PropId.Builder> builder) {
      builder.accept(this);
      return this;
    }

    public PropId build() {
      Objects.requireNonNull(propName, "name must be provided, cannot be null");

      if (Objects.isNull(id) || id.isEmpty()) {
        return new PropId(propName, scope, TableId.of(""), NamespaceId.of(""));
      }

      Pair<String,String> p = Tables.qualify(id, Namespace.DEFAULT.name());

      TableId tableId;
      NamespaceId namespaceId;

      if (scope.equals(Scope.NAMESPACE) && p.getFirst().isEmpty()) {
        namespaceId = NamespaceId.of(p.getSecond());
        tableId = TableId.of("");
      } else {
        namespaceId = NamespaceId.of(p.getFirst());
        tableId = TableId.of(p.getSecond());
      }

      return new PropId(propName, scope, tableId, namespaceId);
    }
  }
}
