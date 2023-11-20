/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UUIDv7Test {

  private static final Logger LOG = LoggerFactory.getLogger(UUIDv7Test.class);

  @Test
  public void byteOrderTest() {
    LOG.info("native:{}", ByteOrder.nativeOrder());
    ByteBuffer origBuff = ByteBuffer.allocate(100);
    LOG.info("buffer: {}", origBuff.order());
  }

  @Test
  public void shifts() {
    LOG.info("all: {}", 0x7fffffff);
    LOG.info("all: {}", String.format("%08x", 999_999_999));
    LOG.info("all: {}", 0xffffffff >>> 20);
    LOG.info("all: {}", 999_999_999 >>> 20);

    LOG.info("all: {}", scale12(999_999_999));
    LOG.info("all: {}", scale12(409_600));

    LOG.info("12_2: {}", scale12_2(999_999_999));
    LOG.info("12_2: {}", scale12_2(409_600));

    LOG.info("mills: {}", TimeUnit.NANOSECONDS.toMicros(409_600));

    LOG.info("Scale: {}", SCALE_NANO * TimeUnit.SECONDS.toNanos(1));

    LOG.info("shift: {}", (1 << 2));

  }

  int scale12(final int v) {
    int x = (int) ((v / 1_000_000_000.0) * 4096);
    return x;
  }

  private static final int NANO_BITS = 4096; // 12 bits.
  private static final double SCALE_NANO = NANO_BITS / 1_000_000_000.0; // max nano + 1.

  int scale12_2(final int v) {
    int x = (int) (v * SCALE_NANO);
    return x;
  }

  @Test
  public void g1() {
    UUIDv7 gen = new UUIDv7();
    for (int i = 0; i < 100_000; i++) {
      UUID u = gen.next();
      if (i % 5000 == 0) {
        LOG.info("U{}: {}", i, u);
      }
    }
    LOG.info("overflow: {}", gen.getOverflowCounter());
    LOG.info("highest: {}", gen.getHighCount());
  }
}
