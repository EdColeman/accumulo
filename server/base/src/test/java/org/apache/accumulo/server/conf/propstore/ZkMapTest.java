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
package org.apache.accumulo.server.conf.propstore;

import static org.junit.Assert.assertEquals;

import java.util.Optional;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkMapTest {

  private static final Logger log = LoggerFactory.getLogger(ZkMapTest.class);

  @Test
  public void jsonRoundTrip() {

    ZkMap m = new ZkMap(TableId.of("1234"), 123);

    m.set(Property.TABLE_BLOOM_ENABLED, "true");
    m.set(Property.TABLE_FILE_MAX, "99");

    byte[] data = m.toJson();

    log.debug("json encoded: {}", new String(data));

    ZkMap r = ZkMap.fromJson(data);

    log.debug("json decoded: {}", r);

    assertEquals("verify same prop value is returned",
        Optional.of(m.get(Property.TABLE_BLOOM_ENABLED).get()),
        r.get(Property.TABLE_BLOOM_ENABLED));

    assertEquals("verify same prop value is returned",
        Optional.of(m.get(Property.TABLE_FILE_MAX).get()), r.get(Property.TABLE_FILE_MAX));

    assertEquals("verify default returned", Optional.empty(), r.get(Property.TABLE_BLOOM_HASHTYPE));

  }

}
