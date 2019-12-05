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
package org.apache.accumulo.server.conf.propstore.dummy;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// sentinel
class SNode {

  private static final Logger log = LoggerFactory.getLogger(SNode.class);

  private Map<TableId,Map<String,String>> propCache = new HashMap<TableId,Map<String,String>>();

  private DummyStore store;

  private final String rootPath;
  private final ZkOps zoo;

  public SNode(final String path, final ZkOps zoo) {
    this.rootPath = path;
    this.zoo = zoo;

    store = new DummyStore(path, this, zoo);
  }

  public String getProp(TableId tableId, String prop) {
    Map<String,String> tableProps = getProps(tableId);
    return tableProps.get(prop);
  }

  public Map<String,String> getProps(TableId tableId) {
    Map<String,String> tableProps = propCache.get(tableId);
    if (tableProps == null) {
      tableProps = store.loadTableProps(tableId);
    }
    return tableProps;
  }

  public boolean setProp(TableId tableId, String name, String value) {
    return false;
  }

  public boolean setProps(TableId tableId, final Map<String,String> tableProps) {
    // TODO merge?
    propCache.put(tableId, tableProps);
    return true;
  }

}
