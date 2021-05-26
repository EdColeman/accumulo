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
package org.apache.accumulo.server.confRewrite.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.PropCacheException;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.accumulo.server.confRewrite.PropChangeListener;
import org.apache.accumulo.server.confRewrite.PropStore;
import org.apache.accumulo.server.confRewrite.cache.PropCache;
import org.apache.accumulo.server.confRewrite.zk.BackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropStoreImpl implements PropStore {

  private static final Logger log = LoggerFactory.getLogger(PropStoreImpl.class);

  private final String instanceId;
  private final PropCache propCache;
  private final BackingStore backingStore;

  public PropStoreImpl(final String instanceId, final PropCache propCache,
      final BackingStore backingStore) {
    this.instanceId = instanceId;
    this.propCache = propCache;
    this.backingStore = backingStore;
  }

  @Override
  public boolean create(final CacheId cacheId, final Map<String,String> props,
      final PropChangeListener listener) throws PropCacheException {

    PropEncoding encoded = new PropEncodingV1();
    encoded.addProperties(props);

    return backingStore.createInStore(cacheId, encoded);
  }

  @Override
  public PropEncoding get(final CacheId cacheId, final PropChangeListener listener)
      throws PropCacheException {

    PropEncoding props = propCache.getProperties(cacheId);
    if (Objects.nonNull(props)) {
      return props;
    }
    return backingStore.readFromStore(cacheId, null);
  }

  @Override
  public boolean updateProperties(final CacheId cacheId, final Map<String,String> props)
      throws PropCacheException {
    return false;
  }

  @Override
  public boolean delete(final CacheId cacheId) throws PropCacheException {
    return false;
  }

  @Override
  public boolean removeProperties(final CacheId cacheId, Collection<String> keys)
      throws PropCacheException {
    return false;
  }

}
