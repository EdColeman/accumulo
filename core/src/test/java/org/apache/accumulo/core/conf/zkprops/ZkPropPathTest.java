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
import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkPropPathTest {

  private static final Logger log = LoggerFactory.getLogger(ZkPropPathTest.class);

  /**
   * exercise path splitting with a path with multiple parts
   */
  @Test
  public void parse1() {

    ZkPropPath p = ZkPropPath.of("/accumulo/instance_id/c/d/e/f/g");

    ZkPropPath.Parts parts = ZkPropPath.parse(p);
    assertEquals("instance_id", parts.getInstance());
    assertEquals("/c/d/e/f", parts.getBase());
    assertEquals("g", parts.getNode());

  }

  /**
   * exercise path with just a node and no parth. i.e /accumulo/instance_id/[NAME]
   */
  @Test
  public void parse2() {

    ZkPropPath p = ZkPropPath.of("/accumulo/instance_id/x");
    ZkPropPath.Parts parts = ZkPropPath.parse(p);

    assertEquals("instance_id", parts.getInstance());
    assertNull("/", parts.getBase());
    assertEquals("x", parts.getNode());

  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidRootTest() {
    ZkPropPath p = ZkPropPath.of("/INVALID/instance_id/c/d/e/f/g");
  }
}
