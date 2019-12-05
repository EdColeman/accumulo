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
package org.apache.accumulo.server.conf.propstore.dummy;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.TableId;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SNodeTest {

  private static final Logger log = LoggerFactory.getLogger(SNodeTest.class);

  String path = "/accumulo/abcd/config/node-0000001";

  @Test
  public void getProp() throws Exception {

    ZooKeeper zks = EasyMock.createStrictMock(ZooKeeper.class);

    // call will check for /accumulo/[instance]/config/node exits - return null, it doesn't
    EasyMock.expect(zks.exists(EasyMock.isA(String.class), EasyMock.anyBoolean())).andReturn(null);

    // call will try to create node with empty data[]
    EasyMock.expect(zks.create(EasyMock.isA(String.class), EasyMock.isA(byte[].class),
        EasyMock.anyObject(), EasyMock.anyObject())).andReturn(path);

    // call will check for /accumulo/[instance]/config/node exits - return null, it doesn't
    EasyMock.expect(zks.exists(EasyMock.isA(String.class), EasyMock.anyBoolean()))
        .andAnswer(new IAnswer<Stat>() {
          @Override
          public Stat answer() throws Throwable {
            Stat s = new Stat();
            s.setMtime(System.currentTimeMillis());
            s.setPzxid(1);
            return s;
          }
        });

    EasyMock.replay(zks);

    ZkOpsImpl zoo = new ZkOpsImpl(zks);

    SNode node = new SNode(path, zoo);

    String v = node.getProp(TableId.of("abc1"), "prop1");

    log.debug("getProp() v: '{}'", v);

    EasyMock.verify(zks);
  }

  @Test
  public void setProp() {

    ZkOps zoo = EasyMock.createStrictMock(ZkOps.class);
    EasyMock.replay(zoo);

    SNode node = new SNode(path, zoo);
    node.setProp(TableId.of("abc1"), "prop1", "value1");

    EasyMock.verify(zoo);
  }

  @Test
  public void x() {
    Map<String,Integer> m = new HashMap<>();

    m.put("a", 1);

    Integer x = m.merge("a", 2, (prev, curr) -> (curr > prev) ? curr : prev);

    // Integer x = m.merge("a", 2, (prev, curr) -> { if(curr > prev){return curr;} else {return
    // prev; }} );
    Integer y = m.merge("b", 22, (prev, curr) -> {
      if (curr > prev) {
        return curr;
      } else {
        return prev;
      }
    });

    int z = (x > y) ? x : y;

    log.info("M: {} -> {}", x, m);
    log.info("M: {} -> {}", y, m);
  }
}
