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

public class PropValueTest {

  private static final Logger log = LoggerFactory.getLogger(SentinelRoot.class);

  @Test
  public void compare(){
    PropValue p1 = PropValue.Builder.builder().withName("n1").withScope(PropScope.DEFAULT).withValue("abc").build();
    PropValue p2 = PropValue.Builder.builder().withName("n1").withScope(PropScope.SYSTEM).withValue("abc").build();

    PropValue p3 = PropValue.Builder.builder().withName("n2").withScope(PropScope.DEFAULT).withValue("abc").build();

    log.info("c:{}", p1.compareTo(p2));
    log.info("c:{}", p2.compareTo(p1));

    log.info("c:{}", p1.compareTo(p3));
    log.info("c:{}", p2.compareTo(p3));
    log.info("c:{}", p3.compareTo(p1));

  }
}
