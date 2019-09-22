/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.functional.util;

import java.nio.charset.StandardCharsets;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metrics2IPCTest {

  private static final Logger log = LoggerFactory.getLogger(Metrics2IPCTest.class);

  @Test
  public void connect() {

    try (Metrics2IPC.IpcSocketSink sink = new Metrics2IPC.IpcSocketSink()) {

      try (Metrics2IPC.IpcSocketSource source = new Metrics2IPC.IpcSocketSource()) {

        Thread.sleep(500);

        log.info("send ?");
        sink.send("?".getBytes(StandardCharsets.UTF_8));

        Thread.sleep(500);
        log.info("send stop");
        sink.send("stop".getBytes(StandardCharsets.UTF_8));

        Thread.sleep(500);

      } catch (Exception ex) {
        log.info("Could not create / connect source", ex);
      }

    } catch (Exception ex) {
      log.info("Could not create / connect sink", ex);
    }
  }
}
