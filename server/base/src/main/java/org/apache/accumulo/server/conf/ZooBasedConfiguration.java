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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.PropCacheChangeListener;
import org.slf4j.Logger;

public abstract class ZooBasedConfiguration extends AccumuloConfiguration {

  private final AtomicBoolean registered = new AtomicBoolean(false);
  private final AtomicReference<Map<String,String>> snapshot = new AtomicReference<>();
  private final AtomicLong updateCounter = new AtomicLong(0);

  private final Logger log;
  private final ServerContext context;
  private final CacheId cacheId;

  protected final AccumuloConfiguration parent;

  protected ZooBasedConfiguration(Logger log, ServerContext context, CacheId cacheId,
      AccumuloConfiguration parent) {
    this.log = requireNonNull(log);
    this.context = requireNonNull(context);
    this.cacheId = requireNonNull(cacheId);
    this.parent = requireNonNull(parent);
  }

  protected Map<String,String> getSnapshot() {
    PropCacheChangeListener listener = cacheId -> {
      var props = context.getPropCache().getProperties(cacheId).get().getAllProperties();
      snapshot.set(props);
      updateCounter.incrementAndGet();
    };
    if (registered.compareAndSet(false, true)) {
      context.getPropCache().register(listener); // register once
      listener.changeEvent(cacheId); // trigger initial values
    }
    return snapshot.get();
  }

  protected String getSnapshotValue(Property property) {
    String key = property.getKey();
    Map<String,String> snapshot = getSnapshot();
    String value = snapshot.get(key);
    if (value != null && property.getType().isValidFormat(value)) {
      return value;
    }
    if (value != null) {
      log.error("Using parent value for {} due to improperly formatted {}: {}", key,
          property.getType(), value);
    }
    return null;
  }

  @Override
  public String get(Property property) {
    String value = getSnapshotValue(property);
    return value != null ? value : parent.get(property);
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    parent.getProperties(props, filter);
    copyFilteredProps(getSnapshot(), filter, props);
  }

  @Override
  public boolean isPropertySet(Property prop) {
    return getSnapshot().containsKey(prop.getKey()) || parent.isPropertySet(prop);
  }

  @Override
  public void invalidateCache() {
    context.getPropCache().clearProperties(cacheId);
  }

  @Override
  public long getUpdateCount() {
    return parent.getUpdateCount() + updateCounter.get();
  }

}
