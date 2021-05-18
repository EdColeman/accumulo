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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import org.apache.accumulo.server.conf2.codec.PropEncoding;

public interface PropStore {

  Optional<PropEncoding> get(CacheId id) throws PropCacheException;

  boolean add(CacheId id, Map<String,String> props) throws PropCacheException;

  boolean create(CacheId id, Map<String,String> props) throws PropCacheException;

  boolean removeProperties(CacheId id, Collection<String> keys) throws PropCacheException;

  // boolean add(CacheId id, String name, String value) throws PropCacheException;

  void clear(CacheId id);

  void clearAll();

  void register(PropWatcher listener);

  void deregister(PropWatcher listener);

  void changeEvent(CacheId id);

  void deleteEvent(CacheId id);

  boolean isReady();

  PropEncoding readFromStore(CacheId id) throws Exception;

  void writeToStore(CacheId id, PropEncoding props);

  void deleteProperties(CacheId id);

  void cleanUp(CacheId id);
}
