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

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZooPropStoreTest {

  private static final Logger log = LoggerFactory.getLogger(ZooPropStoreTest.class);

  private final String unoInstId = "2b0234bb-c157-4de5-bed0-0446528f50e8";

  private static transient boolean haveZookeeper = false;

  private static ZooKeeper zookeeper;

  @BeforeClass public static void init(){
    try {
      zookeeper = new ZooKeeper("localhost:2181", 10_000, new SessionWatcher());
      haveZookeeper = true;
    } catch (IOException ex) {
      log.info("Failed to connect to zookeeper - these tests should be skipped.", ex);
     haveZookeeper = false;
    }
  }

  @AfterClass public static void close(){
    if(haveZookeeper){
      try {
        zookeeper.close();
      }catch(InterruptedException ex){
        Thread.currentThread().interrupt();
      }
    }
  }

  @Test public void simpleStore() throws Exception {

    if(!haveZookeeper){
      log.info("Skipping test. Could not connect to a zookeeper on localhost:2181");
    }

    // zookeeper.exists("", false);

  }

  @Test public void upgradeTest() throws Exception {

    if (!haveZookeeper) {
      log.info("Skipping test. Could not connect to a zookeeper on localhost:2181");
    }

    // original path
    String tableId = "2";

    String originalPath = String.format("/accumulo/%s/tables/%s/conf", unoInstId, tableId);
    String newPath = String.format("/accumulo/%s/tables/%s/conf2", unoInstId, tableId);

    Stat original = zookeeper.exists(originalPath, false);
    if(Objects.nonNull(original)){

      PropEncoding props = new PropEncodingV1(1, true, Instant.now());

      List<String> children = zookeeper.getChildren(originalPath, false);

      for(String propName : children){
        byte[] data = zookeeper.getData(originalPath + "/" + propName,null, null);
        props.addProperty(propName, new String(data, StandardCharsets.UTF_8));
      }

      log.info("Props: {}", props.print(true));

      
    }

  }

  private static class SessionWatcher implements Watcher {

    private static final Logger log = LoggerFactory.getLogger(SessionWatcher.class);

    @Override public void process(WatchedEvent watchedEvent) {
      log.debug("Received session event {}", watchedEvent);
    }
  }
}
