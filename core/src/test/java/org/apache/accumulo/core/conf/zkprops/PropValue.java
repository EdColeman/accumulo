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

public class PropValue implements Comparable<PropValue>{

  final String name;
  final PropScope scope;
  final String value;

  private PropValue(final String name, final PropScope scope, final String value){
    this.name = Objects.requireNonNull(name, "Property name must be provided, cannot be null");
    this.scope = Objects.requireNonNull(scope, "Scope must be provided,cannot be null");
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public PropScope getScope() {
    return scope;
  }

  public String getValue() {
    return value;
  }

  public static class Builder {

    String name;
    PropScope scope;
    String value;

    static Builder builder(){
      return new Builder();
    }

    public Builder withName(final String name){
      this.name = name;
      return this;
    }
    public Builder withScope(final PropScope scope){
      this.scope = scope;
      return this;
    }
    public Builder withValue(final String value){
      this.value = value;
      return this;
    }

    public PropValue build(){
      this.name = Objects.requireNonNull(name, "Property name must be provided, cannot be null");
      this.scope = Objects.requireNonNull(scope, "Scope must be provided,cannot be null");
      this.value = value;
         return new PropValue(name, scope, value);
    }
  }
  private static Comparator<PropValue> compareNameThenScope = Comparator.comparing(PropValue::getName)
      .thenComparingInt(p -> p.getScope().ordinal());

  @Override public int compareTo(PropValue other) {
    return compareNameThenScope.compare(this,other);
  }
}
