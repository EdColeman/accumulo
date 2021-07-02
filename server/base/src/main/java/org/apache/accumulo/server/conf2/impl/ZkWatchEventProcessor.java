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

import java.util.Set;

import org.apache.accumulo.server.conf2.PropCacheId1;
import org.apache.accumulo.server.conf2.PropChangeListener;

public abstract class ZkWatchEventProcessor implements Runnable {

  private final PropCacheId1 propCacheId1;
  private final Set<PropChangeListener> listeners;

  ZkWatchEventProcessor(final PropCacheId1 propCacheId1, final Set<PropChangeListener> listeners) {
    this.propCacheId1 = propCacheId1;
    this.listeners = listeners;
  }

  public static class ZkChangeEventProcessor extends ZkWatchEventProcessor {

    ZkChangeEventProcessor(final PropCacheId1 propCacheId1,
        final Set<PropChangeListener> listeners) {
      super(propCacheId1, listeners);
    }

    @Override
    public void run() {
      super.listeners.forEach(listener -> listener.changeEvent(super.propCacheId1));
    }
  }

  public static class ZkDeleteEventProcessor extends ZkWatchEventProcessor {

    ZkDeleteEventProcessor(final PropCacheId1 propCacheId1,
        final Set<PropChangeListener> listeners) {
      super(propCacheId1, listeners);
    }

    @Override
    public void run() {
      super.listeners.forEach(listener -> listener.deleteEvent(super.propCacheId1));
    }
  }
}
