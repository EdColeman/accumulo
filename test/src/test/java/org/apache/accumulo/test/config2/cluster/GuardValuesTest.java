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
package org.apache.accumulo.test.config2.cluster;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.newCapture;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.accumulo.server.conf2.impl.ZooPropStore;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuardValuesTest {

  private static final Logger log = LoggerFactory.getLogger(GuardValuesTest.class);

  private ServerContext mockContext = null;

  @Before
  public void setup() {

    mockContext = mock(ServerContext.class);
    expect(mockContext.getInstanceID()).andReturn(UUID.randomUUID().toString()).anyTimes();

    replay(mockContext);
  }

  @Test
  public void readTest() {

    ExpectedValues locks = new ExpectedValues(mockContext, 3);
    assertEquals(3, locks.numLocks());

    ArrayList<CacheId> array = new ArrayList<>(locks.getIds());

    CacheId id0 = array.get(0);

    ExpectedProps v = locks.readExpected(id0);
    assertEquals(0, v.getExpectedInt());
    assertFalse(v.isUpdating());

    verify(mockContext);
  }

  @Test
  public void writeTest() throws Exception {

    ZooPropStore mockStore = mock(ZooPropStore.class);
    Capture<CacheId> anId = newCapture();
    Capture<Map<String,String>> props = newCapture();
    expect(mockStore.readFromStore(capture(anId))).andReturn(new PropEncodingV1());
    expect(mockStore.setProperties(anyObject(CacheId.class), capture(props))).andReturn(true)
        .anyTimes();

    replay(mockStore);

    ExpectedValues expectedValues = new ExpectedValues(mockContext, 3);
    assertEquals(3, expectedValues.numLocks());

    ArrayList<CacheId> array = new ArrayList<>(expectedValues.getIds());
    CacheId id1 = array.get(1);

    expectedValues.updateStore(mockStore, id1);

    // TODO - confirm this is a valid test sequence.
    verify(mockStore);

  }
}
