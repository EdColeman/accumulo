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
package org.apache.accumulo.server.conf;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.dataImpl.thrift.IterInfo;

import com.google.common.collect.ImmutableMap;

public class ParsedIteratorConfig {
  private final List<IterInfo> tableIters;
  private final Map<String,Map<String,String>> tableOpts;
  private final String context;

  ParsedIteratorConfig(List<IterInfo> ii, Map<String,Map<String,String>> opts, String context) {
    this.tableIters = List.copyOf(ii);
    var imb = ImmutableMap.<String,Map<String,String>>builder();
    for (Entry<String,Map<String,String>> entry : opts.entrySet()) {
      imb.put(entry.getKey(), Map.copyOf(entry.getValue()));
    }
    tableOpts = imb.build();
    this.context = context;
  }

  public List<IterInfo> getIterInfo() {
    return tableIters;
  }

  public Map<String,Map<String,String>> getOpts() {
    return tableOpts;
  }

  public String getClassLoaderContext() {
    return context;
  }
}
