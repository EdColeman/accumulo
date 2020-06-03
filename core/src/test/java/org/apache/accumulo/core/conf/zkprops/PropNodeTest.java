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

import java.io.IOException;
import java.nio.ByteBuffer;

public class PropNodeTest {

  private static final Logger log = LoggerFactory.getLogger(PropNodeTest.class);

  @Test public void jsonTest() {
    PropNode n1 = createSample1();

    log.debug("N1:{}", n1);
    String j = n1.toJson();

    log.debug("len: {}, json: {}", j.length(), j);

    PropNode r1 = PropNode.fromJson(n1.toJson());
    log.debug("R1:{}", r1);
  }

  @Test public void bytesTest() throws IOException {
    PropNode n1 = createSample1();

    byte[] b = n1.toByteBuffer();

    log.debug("len: {}, R:{}", b.length, PropNode.fromBytes(b));

  }

  private PropNode createSample1() {
    PropNode n1 = new PropNode();

    n1.setProp("a", "123");
    n1.setProp("b", "234");
    return n1;
  }
}
