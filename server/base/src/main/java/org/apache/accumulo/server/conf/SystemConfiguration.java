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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.CacheId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemConfiguration extends ZooBasedConfiguration {

  private static final Logger log = LoggerFactory.getLogger(SystemConfiguration.class);

  // props that are read into this do not change until restart;
  // to be placed here, props must be retrieved with get(Property), not getProperties(...)
  // getProperties(...) will show the current value that will take effect on the next restart,
  // but repeated calls to get(Property) will show the current (fixed) value in effect
  private final Map<String,String> requiresRestartProps =
      Collections.synchronizedMap(new HashMap<>());

  // should only instantiate once, in ServerConfigurationFactory
  SystemConfiguration(ServerContext context) {
    super(log, context, CacheId.forSystem(context), context.getSiteConfiguration());
  }

  // props that can be stored in ZK are attempted to be retrieved from there first
  // any missing props in ZK, or props that can't be stored there, go to the parent
  private String _get(Property prop) {
    String value = Property.isValidZooPropertyKey(prop.getKey()) ? getSnapshotValue(prop) : null;
    return value != null ? value : parent.get(prop);
  }

  @Override
  public String get(Property property) {
    if (Property.requiresRestartPropertyKey(property)) {
      return requiresRestartProps.computeIfAbsent(property.getKey(), k -> _get(property));
    }
    return _get(property);
  }

  @Override
  public boolean isPropertySet(Property prop) {
    return requiresRestartProps.containsKey(prop.getKey()) || super.isPropertySet(prop);
  }

}
