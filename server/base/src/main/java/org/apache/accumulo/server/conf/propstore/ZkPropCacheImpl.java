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
package org.apache.accumulo.server.conf.propstore;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;

public class ZkPropCacheImpl {

  private final Map<Property,String> defaultProps;
  private final Map<Property,String> systemProps;
  private final Map<NamespaceId,Map<Property,String>> namespaceProps;
  private final Map<TableId,Map<Property,String>> tableProps;

  private static ZkPropCacheImpl _instance = null;

  private BackingStore store;

  private ZkPropCacheImpl() {
    defaultProps = new ConcurrentHashMap<>();
    systemProps = new ConcurrentHashMap<>();
    namespaceProps = new ConcurrentHashMap<>();
    tableProps = new ConcurrentHashMap<>();
  }

  public void registerStore(final BackingStore store) {
    this.store = store;
  }

  public Optional<String> get(final NamespaceId namespaceId, final TableId tableId,
      Property property) {

    Map<Property,String> tpropMap = tableProps.get(tableId);
    if (Objects.isNull(tpropMap)) {
      tpropMap = load(tableId);
    }

    String value = tpropMap.get(property);
    if (Objects.nonNull(value)) {
      return Optional.of(value);
    }

    // check namespace

    // check system

    // return default

    return Optional.of(Property.getPropertyByKey(property.getKey()).getDefaultValue());

  }

  private Map<Property,String> load(final TableId tableId) {
    return store.load(tableId);
  }

  public static synchronized ZkPropCacheImpl getInstance() {
    if (_instance == null) {
      _instance = new ZkPropCacheImpl();
    }
    return _instance;
  }

  public void setTableProps(TableId tableName, Map<Property,String> map) {
    tableProps.put(tableName, map);
  }
}
