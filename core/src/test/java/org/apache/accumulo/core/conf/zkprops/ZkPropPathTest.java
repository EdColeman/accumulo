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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ZkPropPathTest {

  private static final Logger log = LoggerFactory.getLogger(ZkPropPathTest.class);

  @Test public void regex(){

    ZkPropPath p = new ZkPropPath("/accumulo/instance_id/c/d/e/f/g");

    assertEquals("instance_id", p.getInstance());
    assertEquals("/c/d/e/f", p.getBase());
    assertEquals("g", p.getId());

  }

  @Test public void regex2(){

    ZkPropPath p = new ZkPropPath("/accumulo/instance_id/x");

    assertEquals("instance_id", p.getInstance());
    assertNull("/", p.getBase());
    assertEquals("x", p.getId());

  }

}
