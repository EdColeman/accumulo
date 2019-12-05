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
package org.apache.accumulo.server.conf.propstore.proto2.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf.propstore.dummy.ZkOps;
import org.apache.accumulo.server.conf.propstore.proto2.TableIdChangeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.UncheckedExecutionException;

public class PropCacheImpl implements PropCache {

  private static final Logger log = LoggerFactory.getLogger(PropCacheImpl.class);

  private final String rootPath;
  private final ZkOps zoo;

  private final static long defaultExpiration = 30L;
  private final static TimeUnit defaultUnits = TimeUnit.MINUTES;

  private final LoadingCache<TableId,PropMap> cache;

  private final List<TableIdChangeListener> tableIdChangeListeners = new ArrayList<>();

  public PropCacheImpl(final String rootPath, final ZkOps zoo) {
    this(rootPath, zoo, defaultExpiration, defaultUnits);
  }

  public PropCacheImpl(final String rootPath, final ZkOps zoo, long expiration, TimeUnit units) {
    super();
    this.rootPath = rootPath;
    this.zoo = zoo;

    CacheLoader<TableId,PropMap> loader = new PropMapZooLoader(rootPath, zoo);

    RemovalListener<TableId,PropMap> removalListener = removalNotification -> {
      log.trace("removed {} - {}", removalNotification.getKey(), removalNotification.getCause());
      tableIdRemoved(removalNotification.getKey());
    };

    cache = CacheBuilder.newBuilder().expireAfterAccess(expiration, units)
        .removalListener(removalListener).build(loader);
  }

  @Override
  public PropMap get(TableId tableId) {
    try {
      return cache.get(tableId);
    } catch (ExecutionException | UncheckedExecutionException ex) {
      log.error("Failed to load values into cache from zookeeper", ex);
    }
    return null;
  }

  @Override
  public void invalidate(final TableId tableId) {
    cache.invalidate(tableId);
  }

  @Override
  public void invalidateAll() {
    cache.invalidateAll();
  }

  void tableIdAdded(final TableId tableId) {
    tableIdChangeListeners.forEach((listener) -> listener.tableIdAdded(tableId));
  }

  void tableIdRemoved(final TableId tableId) {
    tableIdChangeListeners.forEach((listener) -> listener.tableIdRemoved(tableId));
  }

  void syncTableIds() {
    tableIdChangeListeners.forEach(TableIdChangeListener::syncAll);
  }

}
