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
  private ZkPropPath path;
  private int version;

  public CacheablePropMap(final ZkPropPath path, final int version) {
    this(path, version, new PropMap(path));
  }

  public CacheablePropMap(final ZkPropPath path, final int version, final PropMap propMap) {
    this.path = path;
    this.version = version;

    this.propMap = propMap;
  }

  public void updateVersion(final int version) {
    this.version = version;
  }

  public ZkPropPath getPath() {
    return path;
  }

  public int getVersion() {
    return version;
  }

  public void setProperty(String propName, String value) {
    propMap.setProperty(propName, value);
  }

  @Override
  public String toString() {
    return "PropMap{" + "path=" + path.canonical() + ", version=" + version + ", propMap=" + propMap + '}';
  }

  public PropMap getPropMap() {
    return propMap;
  }
}
