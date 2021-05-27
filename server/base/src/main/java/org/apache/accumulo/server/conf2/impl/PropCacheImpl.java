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
package org.apache.accumulo.server.conf2.impl;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.server.conf2.PropCache;
import org.apache.accumulo.server.conf2.PropCacheId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;

public class PropCacheImpl implements PropCache {

  private final Map<PropCacheId,PropEncoding> cache = new ConcurrentHashMap<>();

  @Override
  public void put(PropCacheId propCacheId, PropEncoding props) {
    cache.put(propCacheId, props);
  }

  @Override
  public Optional<PropEncoding> get(PropCacheId propCacheId) {
    return Optional.ofNullable(cache.get(propCacheId));
  }

  @Override
  public void remove(final PropCacheId propCacheId) {
    cache.remove(propCacheId);
  }

  @Override
  public void removeAll() {
    cache.clear();
  }

}
