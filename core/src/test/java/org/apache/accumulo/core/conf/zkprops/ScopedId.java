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

import java.util.function.Consumer;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;

public class ScopedId {
  final String name;
  final PropScope scope;
  final TableId tableId;
  final NamespaceId namespaceId;

  public ScopedId(String name, PropScope scope, TableId tableId, NamespaceId namespaceId) {
    this.name = name;
    this.scope = scope;
    this.tableId = tableId;
    this.namespaceId = namespaceId;
  }

  @Override
  public String toString() {
    return "ScopeId{" + "name='" + name + '\'' + ", scope=" + scope + ", tableId=" + tableId
        + ", namespaceId=" + namespaceId + '}';
  }

  public static class Builder {
    String name;
    TableId tableId;
    NamespaceId namespaceId;
    PropScope scope;

    public ScopedId.Builder with(Consumer<ScopedId.Builder> builder) {
      builder.accept(this);
      return this;
    }

    public ScopedId build() {
      return new ScopedId(name, scope, tableId, namespaceId);
    }
  }
}
