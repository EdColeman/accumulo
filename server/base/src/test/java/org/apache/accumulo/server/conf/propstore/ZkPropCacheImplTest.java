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
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkPropCacheImplTest {

  private static final Logger log = LoggerFactory.getLogger(ZkPropCacheImplTest.class);

  @Test
  public void get() {

    BackingStore mockStore = EasyMock.createStrictMock(BackingStore.class);
    ZkPropCacheImpl props = ZkPropCacheImpl.getInstance();
    props.registerStore(mockStore);

    EasyMock.expect(mockStore.load(TableId.of("123")))
        .andAnswer(new IAnswer<Map<Property,String>>() {
          @Override
          public Map<Property,String> answer() throws Throwable {
            Map<Property,String> props = new HashMap<>();
            props.put(Property.TABLE_BLOOM_ENABLED, "true");
            return props;
          }
        }).anyTimes();

    EasyMock.replay(mockStore);

    Optional<String> fileMax = props.get(null, TableId.of("123"), Property.TABLE_FILE_MAX);
    log.debug("Result: {}", fileMax.isPresent() ? fileMax.get() : "none");
    assertTrue(fileMax.isPresent());
    assertEquals(Property.TABLE_FILE_MAX.getDefaultValue(), fileMax.get());

    Optional<String> bloomEnabled =
        props.get(null, TableId.of("123"), Property.TABLE_BLOOM_ENABLED);
    log.debug("Result: {}", bloomEnabled.isPresent() ? bloomEnabled.get() : "none");
    assertTrue(bloomEnabled.isPresent());
    assertEquals("true", bloomEnabled.get());

    EasyMock.verify(mockStore);
  }
}
