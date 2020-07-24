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
package org.apache.accumulo.core.conf.zkprops;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a PropStore that keeps values in memory to aid in development and testing.
 */
public class PropMemStore implements PropStore {

  private static final Logger log = LoggerFactory.getLogger(PropMemStore.class);

  private Map<String,PropMap> store = new HashMap<>();


  @Override public Optional<String> getProperty(ZkPropPath path, String name) {
    return Optional.empty();
  }

  @Override public Map<String,String> getAll(ZkPropPath path) {
    return null;
  }

  @Override
  public void setProperty(ZkPropPath path, String propName, String value) {

    // if node in local cache, use node id
    // PropMap node = store.computeIfAbsent(path, n -> lookup(path));

    // node.setProperty(propName, value);

    // save(node);

    // use node id
    // node.setProp(propName, value);

    // catch and handle KeeperException.BadVersion

    // ? if same, update anyway?

  }

  @Override
  public void setProperties(Collection<Map.Entry<String,String>> properties) {

  }

  @Override public void deleteProp(ZkPropPath path, String propName) {

  }

  @Override public void deleteAll(ZkPropPath path) {

  }

  @Override public void cloneProperties(ZkPropPath source, ZkPropPath dest) {

  }


}
