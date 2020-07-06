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

import java.util.Comparator;
import java.util.Objects;
import java.util.function.Consumer;

public class PropValue implements Comparable<PropValue> {

  final String name;
  final PropId.Scope scope;
  final String value;

  private PropValue(final String name, final PropId.Scope scope, final String value) {
    this.name = Objects.requireNonNull(name, "Property name must be provided, cannot be null");
    this.scope = Objects.requireNonNull(scope, "Scope must be provided,cannot be null");
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public PropId.Scope getScope() {
    return scope;
  }

  public String getValue() {
    return value;
  }

  public static class Builder {
    String name;
    PropId.Scope scope;
    String value;

    public Builder with(Consumer<Builder> builder) {
      builder.accept(this);
      return this;
    }

    public PropValue build() {
      return new PropValue(name, scope, value);
    }
  }

  private static Comparator<PropValue> compareNameThenScope =
      Comparator.comparing(PropValue::getName).thenComparingInt(p -> p.getScope().ordinal());

  @Override
  public int compareTo(PropValue other) {
    return compareNameThenScope.compare(this, other);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    PropValue propValue = (PropValue) o;
    return name.equals(propValue.name) && scope == propValue.scope
        && Objects.equals(value, propValue.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, scope, value);
  }

  @Override
  public String toString() {
    return "PropValue{" + "name='" + name + '\'' + ", scope=" + scope + ", value='" + value + '\''
        + '}';
  }
}
