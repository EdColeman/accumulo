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
package org.apache.accumulo.server.confRewrite;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.PropCacheException;
import org.apache.accumulo.server.conf2.codec.PropEncoding;

public interface PropStore {

  /**
   * Retrieve the properties for the provided cacheId from the backing store.
   *
   * @param cacheId
   *          the cache id.
   * @return the properties or null if not present in backing store.
   * @throws PropCacheException
   *           If an error occurs reading from the backing store.
   */
  PropEncoding get(CacheId cacheId) throws PropCacheException;

  /**
   * Retrieve the properties for the provided cacheId from the backing store and register a listener
   * for changes (modifications, deletions)
   *
   * @param cacheId
   *          the cache id
   * @param listener
   *          the listener that notified of changes (modified, deleted)
   * @return the properties or null if not present in backing store.
   * @throws PropCacheException
   *           If an error occurs reading from the backing store.
   */
  PropEncoding get(CacheId cacheId, PropChangeListener listener) throws PropCacheException;

  /**
   * Create a map of properties (key, value pairs) and associate them with the cache id in the
   * backing store and register a listener for changes (modifications, deletions)
   *
   * @param cacheId
   *          the cache id.
   * @param props
   *          a map of key, value pairs
   * @param listener
   *          the listener that notified of changes (modified, deleted)
   * @return true if created, false if the properties exist in the backing store.
   * @throws PropCacheException
   *           If an error occurs in the backing store.
   */
  boolean create(CacheId cacheId, Map<String,String> props, PropChangeListener listener)
      throws PropCacheException;

  /**
   * Create a map of properties (key, value pairs) and associate them with the cache id in the
   * backing store.
   *
   * @param cacheId
   *          the cache id.
   * @param props
   *          a map of key, value pairs
   * @return true if created, false if the properties exist in the backing store.
   * @throws PropCacheException
   *           If an error occurs in the backing store.
   */
  boolean create(CacheId cacheId, Map<String,String> props) throws PropCacheException;

  /**
   * Delete the properties associated with the cache id.
   *
   * @param cacheId
   *          the cache id.
   * @return true if deleted, false if did not exist in the backing store
   * @throws PropCacheException
   *           If an error occurs in the backing store.
   */
  boolean delete(CacheId cacheId) throws PropCacheException;

  /**
   * Add or modify existing properties associated with a cache id. Properties (key, value pairs)
   * that do not current exist are added. Properties keys that do exist, the values are replaced.
   *
   * @param cacheId
   *          the cache id.
   * @param props
   *          a map of key value pairs.
   * @return true if updated, false if not
   * @throws PropCacheException
   *           If an exception occurs in the backing store.
   */
  boolean updateProperties(CacheId cacheId, Map<String,String> props) throws PropCacheException;

  /**
   * Remove the properties by provided keys that are associated with the cache id.
   *
   * @param cacheId
   *          the cache id.
   * @param keys
   *          the keys of the property names to be removed/
   * @return true if successful, false if not.
   * @throws PropCacheException
   *           If an exception occurs in the backing store.
   */
  boolean removeProperties(CacheId cacheId, Collection<String> keys) throws PropCacheException;

}
