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
package org.apache.accumulo.server.conf;

import static java.util.function.Predicate.not;

import java.util.Map;
import java.util.function.Predicate;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.CacheId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamespaceConfiguration extends ZooBasedConfiguration {

  private static final Logger log = LoggerFactory.getLogger(NamespaceConfiguration.class);

  private final NamespaceId namespaceId;

  // should only instantiate once per namespace, in ServerConfigurationFactory
  protected NamespaceConfiguration(NamespaceId namespaceId, ServerContext context) {
    super(log, context, CacheId.forNamespace(context, namespaceId), context.getConfiguration());
    this.namespaceId = namespaceId;
  }

  public NamespaceId getNamespaceId() {
    return namespaceId;
  }

  @Override
  public String get(Property property) {
    String value = getSnapshotValue(property);
    if (value != null) {
      return value;
    }
    if (getNamespaceId().equals(Namespace.ACCUMULO.id())
        && isIteratorOrConstraint(property.getKey())) {
      // ignore iterators from parent if system namespace
      return null;
    }
    return parent.get(property);
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    // exclude system iterators/constraints from the system namespace
    // so they don't affect the metadata or root tables.
    if (getNamespaceId().equals(Namespace.ACCUMULO.id())) {
      parent.getProperties(props, not(this::isIteratorOrConstraint).and(filter));
      copyFilteredProps(getSnapshot(), filter, props);
    } else {
      super.getProperties(props, filter);
    }
  }

  private boolean isIteratorOrConstraint(String key) {
    return key.startsWith(Property.TABLE_ITERATOR_PREFIX.getKey())
        || key.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey());
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "(NamespaceId:" + getNamespaceId() + ")";
  }

}
