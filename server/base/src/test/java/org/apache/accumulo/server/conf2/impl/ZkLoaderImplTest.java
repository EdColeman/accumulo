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
package org.apache.accumulo.server.conf2.impl;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;

import java.util.Optional;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.PropCache;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.accumulo.server.conf2.codec.PropEncodingV1;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkLoaderImplTest {

  private static final Logger log = LoggerFactory.getLogger(ZkLoaderImplTest.class);

  @Test
  public void simple() throws Exception {

    var instanceId = UUID.randomUUID().toString();
    var cacheId = new CacheId(instanceId, null, TableId.of("a"));
    var zkDataPath = "/accumulo/" + instanceId + Constants.ZENCODED_CONFIG_ROOT + "/-::a";

    PropEncoding props = new PropEncodingV1();
    props.addProperty("key_1", "value_1");

    Capture<Stat> stat = EasyMock.newCapture();

    ZooKeeper zooKeeper = EasyMock.mock(ZooKeeper.class);
    ZkEventProcessor cache = EasyMock.mock(PropCache.class);

    expect(zooKeeper.getData(eq(zkDataPath), anyObject(), EasyMock.capture(stat)))
        .andAnswer(new IAnswer<byte[]>() {
          @Override
          public byte[] answer() throws Throwable {
            stat.getValue().setMzxid(1234);
            return props.toBytes();
          }
        });

    EasyMock.replay(zooKeeper);

    ZkLoaderImpl zkLoader = new ZkLoaderImpl(zooKeeper, cache);
    Optional<PropEncoding> data = zkLoader.readFromZooKeeper(cacheId);

    log.info("Received: {}", data.get().print(true));
  }
}
