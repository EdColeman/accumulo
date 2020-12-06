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

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemPropStore implements PropStore {

  private static final Logger log = LoggerFactory.getLogger(MemPropStore.class);

  private final Map<CacheId,PropEncoding> store = new HashMap<>();
  private final PropertyChangeSupport propChangeSupport;

  public MemPropStore() {
    propChangeSupport = new PropertyChangeSupport(this);
  }

  @Override
  public PropEncoding get(CacheId id, PropertyChangeListener pcl) {
    return store.get(id);
  }

  @Override
  public void set(CacheId id, PropEncoding props) {
    store.put(id, props);
    log.debug("changing {} - {}", id, id.hashCode());

    propChangeSupport.firePropertyChange(id.asKey(), -1, props.getDataVersion());

  }

  @Override
  public void registerForChanges(PropertyChangeListener notifier) {
    propChangeSupport.addPropertyChangeListener(notifier);
    log.debug("Listeners: {}", Arrays.asList(propChangeSupport.getPropertyChangeListeners()));
  }
}
