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

import static java.util.Objects.requireNonNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metrics2ProtocolHandler {

  private static final int BUFFER_SIZE = 64 * 1024;

  private static final Logger log = LoggerFactory.getLogger(Metrics2ProtocolHandler.class);

  private final DataInputStream in;
  private final DataOutputStream out;

  private final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

  public Metrics2ProtocolHandler(final DataInputStream in, final DataOutputStream out) {
    requireNonNull(in);
    requireNonNull(out);
    this.in = in;
    this.out = out;
  }

  private final Semaphore barrier = new Semaphore(1);

  public String read() {

    try {

      barrier.acquire();

      log.debug("start blocking read");

      while(in.available() < 4) {
        Thread.sleep(500);
      }

      int length = in.readInt();

      if (length > BUFFER_SIZE) {
        throw new IllegalArgumentException(
            String.format("Message length %d exceeds buffer size %d", length, BUFFER_SIZE));
      }

      log.debug("Expecting {}", length);

      buffer.clear();

      in.readFully(buffer.array(), 0, length);

      buffer.flip().limit(length);

      return StandardCharsets.UTF_8.decode(buffer).toString();

    } catch (InterruptedException iex) {
      Thread.currentThread().interrupt();
    } catch (IOException ex) {
      log.debug("read failed");
      return "";
      // throw new IllegalStateException("read failed", ex);
    } finally {
      barrier.release();
    }
    return "";
  }

  public synchronized void send(final byte[] payload) {
    try {

      int length = payload.length;

      if (length > BUFFER_SIZE) {
        throw new IllegalArgumentException(
            String.format("Message length %d exceeds buffer size %d", length, BUFFER_SIZE));
      }

      out.writeInt(length);
      out.write(payload, 0, length);
      out.flush();

    } catch (IOException ex) {
      throw new IllegalStateException("Send failed", ex);
    }
  }

}
