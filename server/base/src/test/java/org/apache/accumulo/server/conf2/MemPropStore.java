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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemPropStore implements PropCache, PropStore {

  private static final Logger log = LoggerFactory.getLogger(MemPropStore.class);

  private final Map<String,PropEncoding> store = new HashMap<>();

  public MemPropStore() {}

  @Override
  public Optional<PropEncoding> get(CacheId id) {
    log.trace("get from map for id: {}", id);
    return Optional.ofNullable(store.get(id.nodeName()));
  }

  @Override
  public boolean add(CacheId id, Map<String,String> props) {
    // writeToStore(id, props);
    return true;
  }

  @Override
  public boolean create(CacheId id, Map<String,String> props) throws PropCacheException {
    return false;
  }

  @Override
  public boolean removeProperties(CacheId id, Collection<String> keys) {
    return false;
  }

  public boolean add(CacheId id, String name, String value) {
    return false;
  }

  @Override
  public void clear(CacheId id) {

  }

  @Override
  public void deleteProperties(CacheId id) {

  }

  @Override
  public void cleanUp(CacheId id) {

  }

  @Override
  public void clearAll() {

  }

  @Override
  public void register(PropWatcher listener) {

  }

  @Override
  public void deregister(PropWatcher listener) {

  }

  @Override
  public void changeEvent(CacheId id) {

  }

  @Override
  public void deleteEvent(CacheId id) {

  }

  @Override
  public boolean isReady() {
    return true;
  }

  @Override
  public PropEncoding readFromStore(CacheId id) {
    log.trace("read id: {} from map", id);
    return store.get(id.nodeName());
  }

  @Override
  public void writeToStore(CacheId id, PropEncoding props) {
    log.trace("write id: {} - props: {} to map", id, props);
    store.put(id.nodeName(), props);
  }

}
