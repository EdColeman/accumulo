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
package org.apache.accumulo.server.conf2;

import static org.junit.Assert.assertEquals;

import java.time.Instant;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropEncodingV1Test {

  private static final Logger log = LoggerFactory.getLogger(PropEncodingV1Test.class);

  @Test
  public void compressedEncodeTest() {

    PropEncoding props = new PropEncodingV1(1, true, Instant.now());
    fillMap(props);

    int ver = props.getDataVersion();

    log.info("Created TS {}", props.getTimestamp());

    byte[] bytes = props.toBytes();

    log.info("Written TS {}", props.getTimestamp());

    assertEquals(props.getDataVersion(), ver + 1);

    log.info("compressed encoded length: {}", bytes.length);
    PropEncodingV1 decoded = new PropEncodingV1(bytes);

    log.info("Decoded:\n{}", decoded.print(true));

  }

  @Test
  public void uncompressedEncodeTest() {

    PropEncoding props = new PropEncodingV1(1, false, Instant.now());
    fillMap(props);

    byte[] bytes = props.toBytes();

    log.info("uncompressed encoded length: {}", bytes.length);

    PropEncodingV1 decoded = new PropEncodingV1(bytes);

    log.info("Decoded:\n{}", decoded.print(true));

  }

  private void fillMap(final PropEncoding props) {
    props.addProperty("key1", "value1");
    props.addProperty("key2", "value2");
    props.addProperty("key3", "value3");
    props.addProperty("key4", "value4");
  }
}
