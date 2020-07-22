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
import java.util.function.Consumer;

public class NodeVersionedId implements Comparable<NodeVersionedId> {

  private final ZkPropPath path;
  private final int dataVersion;

  private NodeVersionedId(final ZkPropPath path, final int dataVersion) {
    this.path = path;
    this.dataVersion = dataVersion;
  }

  public ZkPropPath getPath() {
    return path;
  }

  public int getDataVersion() {
    return dataVersion;
  }

  @Override
  public String toString() {
    return "NodeVersionedId{" + "path='" + path.canonical() + '\'' + ", dataVersion=" + dataVersion
        + '}';
  }

  /**
   * Test is an instance equals this instance - dataVersion is ignored in for equals, hashCode and
   * compareTo.
   *
   * @param o
   *          an instance to compare
   * @return true if paths contain same characters
   */
  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    NodeVersionedId that = (NodeVersionedId) o;
    return Objects.equals(path, that.path);
  }

  /**
   * Generate a hash code for the instance. dataVersion is ignored in the calculation.
   *
   * @return a hashcode for this instance.
   */
  @Override
  public int hashCode() {
    return Objects.hash(path);
  }

  /**
   * Compare this and another instances. dataVersion is ignored.
   *
   * @param other
   *          an instance to compare.
   * @return -1, 0 or 1 if less than, equal or greater than
   */
  @Override
  public int compareTo(NodeVersionedId other) {
    Comparator<NodeVersionedId> comparator = comparing(NodeVersionedId::getPath);
    return comparator.compare(this, other);
  }

  public static class Builder {

    ZkPropPath path;
    int dataVersion = -1;

    public NodeVersionedId.Builder with(Consumer<NodeVersionedId.Builder> builder) {
      builder.accept(this);
      return this;
    }

    public NodeVersionedId.Builder updateVersion(final NodeVersionedId other,
        final int dataVersion) {
      path = other.path;
      this.dataVersion = dataVersion;

      return this;
    }

    public NodeVersionedId build() {
      Objects.requireNonNull(path, "name must be provided, cannot be null");

      return new NodeVersionedId(path, dataVersion);
    }
  }
}
