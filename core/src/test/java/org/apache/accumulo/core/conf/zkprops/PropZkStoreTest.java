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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class PropZkStoreTest {

  private static ZooKeeperTestingServer szk = null;

  private static String MOCK_ZK_ROOT = "/accumulo/1234/props";

  @BeforeClass
  public static void setupZk() {
    // using default zookeeper port - we don't have a full configuration
    szk = new ZooKeeperTestingServer();
    szk.initPaths(MOCK_ZK_ROOT);
  }

  @AfterClass
  public static void shutdownZK() throws Exception {
    szk.close();
  }

  @Test
  public void emptyStore() {
    PropStore store = new PropZkStore();
    PropData data = store.get("/unknown");
  }

  @Test
  public void simpleStore() {

  }
}
