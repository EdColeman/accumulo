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

import java.util.Map;
import java.util.TreeMap;

import com.google.gson.Gson;

public class PropData {

  private NodeVersionedId id;
  private final Map<String,String> propMap = new TreeMap<>();

  private static final Gson gson = null;

  public PropData(final String path, final int version) {
    this.id = new NodeVersionedId.Builder().with($ -> {
      $.path = path;
      $.dataVersion = version;
    }).build();

  }

  public void updateVersion(final int version) {
    id = new NodeVersionedId.Builder().updateVersion(id, version).build();
  }

  public int getVersion() {
    return id.getDataVersion();
  }

  public void setProperty(String propName, String value) {
    propMap.put(propName, value);
  }

  @Override
  public String toString() {
    return "PropData{" + "id=" + id + ", propMap=" + propMap + '}';
  }
}
