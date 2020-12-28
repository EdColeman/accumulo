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
package org.apache.accumulo.server.conf2;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;

public class Notifier implements PropertyChangeListener {

  private static final Logger log = LoggerFactory.getLogger(Notifier.class);

  private final Set<CacheId> listeners = new HashSet<>();

  private final LoadingCache<CacheId,PropEncoding> backingCache;

  public Notifier(final LoadingCache<CacheId,PropEncoding> cache) {
    this.backingCache = cache;
  }

  public void onRemoval(RemovalNotification<CacheId,PropEncoding> removalNotification) {
    log.info("Removing {} from notifications that we care about", removalNotification.getKey());
    listeners.remove(removalNotification.getKey());
  }

  @Override
  public void propertyChange(PropertyChangeEvent pce) {
    log.debug("Received prop change event {}", pce);
    CacheId id = CacheId.fromKey(pce.getPropertyName());
    if (listeners.contains(id)) {
      log.warn("We care about:{}", id);
    }
    log.warn("HAVE key: {}, {}", id, backingCache.asMap().containsKey(id));
    backingCache.invalidate(id);
  }

  public void monitor(CacheId id) {
    // also watch namespace.
    listeners.add(id);
  }
}
