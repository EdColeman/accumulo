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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class SentinelRoot {

  private static final Logger log = LoggerFactory.getLogger(SentinelRoot.class);

  private static final int NUM_NS_SENTINEL_NODES = 8;
  private static final int NS_MASK = NUM_NS_SENTINEL_NODES - 1;

  private static final int NUM_TABLE_SENTINEL_NODES = 16;
  private static final int TABLE_MASK = NUM_TABLE_SENTINEL_NODES - 1;

  private static final HashFunction hasher = Hashing.murmur3_32();

  private final String[] tablePropNodes = new String[NUM_TABLE_SENTINEL_NODES];
  private final String[] namespacePropNodes = new String[NUM_NS_SENTINEL_NODES];

  public final String ZK_SENTINEL_ROOT;
  public final String ZK_PROPS_BASE;
  public final String ZK_SYSTEM_PROPS_PATH;
  public final String ZK_NS_PROPS_BASE;
  public final String ZK_TABLE_PROPS_BASE;

  // stub for development
  private PropStore store = new PropMemStore();

  public SentinelRoot(final String instance) {

    ZK_PROPS_BASE = "/accumulo/" + instance + "/props";
    ZK_SENTINEL_ROOT = ZK_PROPS_BASE + "/sentinel";
    ZK_SYSTEM_PROPS_PATH = ZK_PROPS_BASE + "/system";
    ZK_NS_PROPS_BASE = ZK_PROPS_BASE + "/namespace/";
    ZK_TABLE_PROPS_BASE = ZK_PROPS_BASE + "/table/";

    // the num nodes needs to be a power of 2 for masking to work as intended.
    assert (NUM_NS_SENTINEL_NODES & (NS_MASK)) == 0;
    assert (NUM_TABLE_SENTINEL_NODES & (TABLE_MASK)) == 0;
  }

  public int lookup(final String id) {
    int bin = hasher.hashString(id, UTF_8).hashCode() & TABLE_MASK;
    return bin;
  }

  public void setSystemProperty(final String propName, final String value) {
    setProperty(PropId.Scope.SYSTEM, ZK_SYSTEM_PROPS_PATH, propName, value);
  }

  public void setNamespaceProperty(final String namespace, final String propName,
      final String value) {
    var nsPath = ZK_NS_PROPS_BASE + namespace;
    setProperty(PropId.Scope.NAMESPACE, nsPath, propName, value);
  }

  public void setTableProperty(final String tablename, final String propName, final String value) {
    var tablePath = ZK_NS_PROPS_BASE + tablename;
    setProperty(PropId.Scope.TABLE, tablePath, propName, value);
  }

  private void setProperty(final PropId.Scope scope, final String path, final String name,
      final String value) {
    store.setProperty(scope, path, name, value);
  }

  public String getProperty(final String name) {
    return "";
  }

}
