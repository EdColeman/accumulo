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

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.PropEncoding;
import org.apache.accumulo.server.conf2.PropCacheId;
import org.apache.accumulo.server.conf2.PropChangeListener;
import org.apache.accumulo.server.conf2.PropStore;
import org.apache.accumulo.server.conf2.PropStoreException;
import org.slf4j.Logger;

public class ZooBasedConfiguration extends AccumuloConfiguration implements PropChangeListener {

  private final Logger log;
  private final ServerContext context;
  private final AccumuloConfiguration parent;
  private final PropCacheId propCacheId;
  private final PropStore propStore;

  private final Map<String,String> fixedProps;

  // private final AtomicReference<Instant> lastUpdate = new AtomicReference<>(Instant.EPOCH);

  public ZooBasedConfiguration(Logger log, ServerContext context, PropCacheId propCacheId,
      AccumuloConfiguration parent) {
    this.log = requireNonNull(log);
    this.context = requireNonNull(context);
    this.propCacheId = requireNonNull(propCacheId);
    this.parent = requireNonNull(parent);

    this.propStore = requireNonNull(context.getPropStore());

    propStore.registerAsListener(propCacheId, this);

    fixedProps = propStore.readFixed();
  }

  public PropCacheId getCacheId() {
    return propCacheId;
  }

  public AccumuloConfiguration getParent() {
    return parent;
  }

  public PropEncoding getProperties() throws PropStoreException {
    PropEncoding props = propStore.get(propCacheId);
    return props;
  }

  @Override
  public void invalidateCache() {
    // if (propCache != null)
    // propCache.clear();
  }

  private String _get(Property property) {
    String key = property.getKey();
    String value = null;
    if (Property.isValidZooPropertyKey(key)) {
      value = getCachedProp(key);
    }

    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null) {
        log.error("Using parent value for {} due to improperly formatted {}: {}", key,
            property.getType(), value);
      }
      value = parent.get(property);
    }
    return value;
  }

  @Override
  public String get(Property property) {
    if (Property.isFixedZooPropertyKey(property)) {
      String val = fixedProps.get(property.getKey());
      Objects.requireNonNull(val,
          "Error reading required fixed property value for " + property.getKey());
      return val;
    } else {
      return _get(property);
    }
  }

  public String getValid(final Property property) {

    String key = property.getKey();
    String value = getCachedProp(key);

    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null) {
        log.error("Using default value for {} due to improperly formatted {}: {}", key,
            property.getType(), value);
      }
      if (parent != null) {
        value = parent.get(property);
      }
    }
    return value;
  }

  @Override
  public boolean isPropertySet(Property prop, boolean cacheAndWatch) {

    if (fixedProps.containsKey(prop.getKey())) {
      return true;
    }

    try {
      PropEncoding encoded = propStore.get(propCacheId);
      if (Objects.nonNull(encoded.getAllProperties().get(prop.getKey()))) {
        return true;
      }
    } catch (PropStoreException pex) {
      throw new IllegalStateException(pex);
    }

    return parent.isPropertySet(prop, cacheAndWatch);
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {

    parent.getProperties(props, filter);

    try {
      PropEncoding encoded = propStore.get(propCacheId);
      for (Map.Entry<String,String> entry : encoded.getAllProperties().entrySet()) {
        if (filter.test(entry.getKey())) {
          props.put(entry.getKey(), entry.getValue());
        }
      }
    } catch (PropStoreException pex) {
      throw new IllegalStateException(pex);
    }
  }

  @Override
  public long getUpdateCount() {

    try {
      PropEncoding encoded = propStore.get(propCacheId);
      // mask truncates the sign extension when the int is cast to a long when negative number
      return (parent.getUpdateCount() << 32) | (0x0000_0000_ffff_ffffL & encoded.getDataVersion());
    } catch (PropStoreException pex) {
      throw new IllegalStateException(pex);
    }
  }

  @Override
  public void changeEvent(PropCacheId watchedId) {
    if (propCacheId.equals(watchedId)) {
      log.trace("Received change event for {}", propCacheId);
    }
  }

  @Override
  public void deleteEvent(PropCacheId watchedId) {
    if (propCacheId.equals(watchedId)) {
      log.trace("Received delete event for {}", propCacheId);
    }
  }

  private String getCachedProp(String key) {
    String value = null;
    try {
      PropEncoding encoded = propStore.get(propCacheId);
      if (Objects.nonNull(encoded)) {
        Map<String,String> props = encoded.getAllProperties();
        value = props.get(key);
      }
    } catch (PropStoreException pex) {
      throw new IllegalStateException(pex);
    }
    return value;
  }

}
