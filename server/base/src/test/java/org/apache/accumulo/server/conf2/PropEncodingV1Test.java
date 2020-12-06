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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropEncodingV1Test {

  private static final Logger log = LoggerFactory.getLogger(PropEncodingV1Test.class);

  @Test public void encodeTest(){

    PropEncodingV1 props = new PropEncodingV1();

    props.add("a.b.c.d.key1", "value1");
    props.add("a.b.c.d.key2", "value2");
    props.add("a.b.c.d.key3", "value3");
    props.add("a.b.c.d.key4", "value4");
    props.add("a.b.c.d.key5", "value5");
    props.add("a.b.c.d.key6", "value6");
    props.add("a.b.c.d.key7", "value7");

    byte[] bytes = props.toBytes();

    log.info("encoded length: {}", bytes.length);

    PropEncodingV1 decoded = PropEncodingV1.fromBytes(bytes);

    log.info("Decoded:\n{}", decoded.print(true));

  }
}
