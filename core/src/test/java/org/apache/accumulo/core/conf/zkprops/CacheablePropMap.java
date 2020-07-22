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

public class CacheablePropMap {

  private final PropMap propMap;
  private NodeVersionedId id;

  public CacheablePropMap(final ZkPropPath path, final int version) {
    this(path, version, new PropMap(path));
  }

  public CacheablePropMap(final ZkPropPath path, final int version, final PropMap propMap) {
    this.id = new NodeVersionedId.Builder().with($ -> {
      $.path = path;
      $.dataVersion = version;
    }).build();

    this.propMap = propMap;
  }

  public void updateVersion(final int version) {
    id = new NodeVersionedId.Builder().updateVersion(id, version).build();
  }

  public ZkPropPath getPath() {
    return id.getPath();
  }

  public int getVersion() {
    return id.getDataVersion();
  }

  public void setProperty(String propName, String value) {
    propMap.setProperty(propName, value);
  }

  @Override
  public String toString() {
    return "PropMap{" + "id=" + id + ", propMap=" + propMap + '}';
  }

  public PropMap getPropMap() {
    return propMap;
  }
}
