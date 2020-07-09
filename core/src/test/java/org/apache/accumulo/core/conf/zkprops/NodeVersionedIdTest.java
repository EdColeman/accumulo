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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class NodeVersionedIdTest {

  @Test
  public void buildDefaultVersion() {

    var path = "/a/b/c/def";
    NodeVersionedId id = new NodeVersionedId.Builder().with($ -> {
      $.path = path;
    }).build();

    assertEquals(-1, id.getDataVersion());
    assertEquals(path, id.getPath());
  }

  @Test
  public void buildNewVersion() {

    var path = "/a/b/c/def";
    var ver = 123;

    NodeVersionedId id = new NodeVersionedId.Builder().with($ -> {
      $.path = path;
      $.dataVersion = ver;
    }).build();

    assertEquals(ver, id.getDataVersion());
    assertEquals(path, id.getPath());
  }

  @Test
  public void updateVersion() {

    var path = "/a/b/c/def";
    var ver = 123;

    NodeVersionedId id = new NodeVersionedId.Builder().with($ -> {
      $.path = path;
      $.dataVersion = ver;
    }).build();

    assertEquals(ver, id.getDataVersion());
    assertEquals(path, id.getPath());

    var ver2 = 456;
    NodeVersionedId id2 = new NodeVersionedId.Builder().updateVersion(id, ver2).build();

    assertEquals(ver2, id2.getDataVersion());
    assertEquals(path, id2.getPath());

  }
}
