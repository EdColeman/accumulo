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

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemPropStore implements PropStore {

  private static final Logger log = LoggerFactory.getLogger(MemPropStore.class);

  private final Map<CacheId,PropEncoding> store = new HashMap<>();

  public MemPropStore() {}

  @Override
  public boolean isReady() {
    return true;
  }

  @Override
  public void enable() {
    // no-op
    log.debug("received notice backend store is connected");
  }

  @Override
  public void disable() {
    // no-op
    log.debug("Received notice backend store is disconnected");
  }

  @Override
  public PropEncoding get(CacheId id, Notifier notifier) throws Exception {
    return null;
  }

  @Override
  public void set(CacheId id, PropEncoding props) {
    store.put(id, props);
    log.debug("changing {} - {}", id, id.hashCode());
  }

  @Override
  public void registerForChanges(Notifier notifier) {}
}
