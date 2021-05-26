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
package org.apache.accumulo.server.confRewrite.impl;

import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.confRewrite.PropStore;
import org.apache.accumulo.server.confRewrite.cache.PropCache;
import org.apache.accumulo.server.confRewrite.zk.BackingStore;
import org.easymock.EasyMock;
import org.junit.Test;

public class PropStoreImplTest {

  @Test
  public void createNoListenerTest() throws Exception {

    String instanceId = UUID.randomUUID().toString();
    PropCache propCache = EasyMock.mock(PropCache.class);

    BackingStore backingStore = EasyMock.mock(BackingStore.class);
    EasyMock.expect(backingStore.createInStore(isA(CacheId.class), isA(PropEncoding.class)))
        .andReturn(true);

    PropStore propStore = new PropStoreImpl(instanceId, propCache, backingStore);

    CacheId tid = CacheId.forTable(instanceId, TableId.of("a"));
    Map<String,String> props = new HashMap<>();
    props.put("key_1", "value_1");

    EasyMock.replay(propCache, backingStore);

    assertTrue(propStore.create(tid, props));

    EasyMock.verify(propCache, backingStore);
  }
}
