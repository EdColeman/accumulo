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

import java.io.IOException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropNodeSerdesTest {

  private static final Logger log = LoggerFactory.getLogger(PropNodeSerdesTest.class);

  private PropStore store = new PropMemStore();

  @Test
  public void jsonTest() {

    PropNode n1 = createTableProp();
    log.debug("N1:{}", n1);

    PropNodeSerdes serdes = new PropNodeSerdes();

    String j = serdes.toJson(n1);

    log.debug("len: {}, json: {}", j.length(), j);

    PropNode r1 = serdes.fromJson(serdes.toJson(n1));
    log.debug("R1:{}", r1);
  }

  @Test
  public void bytesTest() throws IOException {
    PropNode n1 = createTableProp();

    log.info("NNN: {}", n1.toString());

    PropNodeSerdes serdes = new PropNodeSerdes();

    byte[] b = serdes.toByteBuffer(n1);

    log.debug("len: {}, R:{}", b.length, serdes.fromBytes(b));

  }

  @Test
  public void writeUncompressedTest() throws IOException {
    PropNode n1 = createTableProp();
    PropNodeSerdes serdes = new PropNodeSerdes();
    serdes.disableCompression();

    log.info("NNN: {}", n1.toString());

    byte[] b = serdes.toByteBuffer(n1);

    log.debug("len: {}, R:{}", b.length, serdes.fromBytes(b));
  }

  /**
   * The use of compression / decompression is encoded with the data written. Check that the data
   * value take precedence when reading.
   *
   * @throws IOException
   *           if error processing occurs
   */
  @Test
  public void checkDataCompressionOnRead() throws IOException {

    PropNode n1 = createTableProp();
    PropNodeSerdes serdes = new PropNodeSerdes();
    serdes.enableCompression();

    byte[] b = serdes.toByteBuffer(n1);

    serdes.disableCompression();

    PropNode n2 = serdes.fromBytes(b);

    assertEquals(n1.getNodeId(), n2.getNodeId());
  }

  @Test
  public void systemPropsTest() throws IOException {

    PropId id = new PropId.Builder().with($ -> {
      $.propName = "SYSTEM";
      $.scope = PropId.Scope.SYSTEM;
    }).build();

    PropNode n1 = new PropNode.Factory().with($ -> {
      $.id = id;
    }).create();

    n1.setProp("a", "123");
    n1.setProp("b", "234");

    log.info("NNN: {}", n1.toString());

    PropNodeSerdes serdes = new PropNodeSerdes();

    byte[] b = serdes.toByteBuffer(n1);

    log.debug("from bytes len: {}, R:{}", b.length, serdes.fromBytes(b));

  }

  private PropNode createTableProp() {

    PropId id = new PropId.Builder().with($ -> {
      $.propName = "abc";
      $.scope = PropId.Scope.TABLE;
      $.id = "foo.bar";
    }).build();

    PropNode n1 = new PropNode.Factory().with($ -> {
      $.id = id;
    }).create();

    n1.setProp("a", "123");
    n1.setProp("b", "234");
    return n1;
  }

}
