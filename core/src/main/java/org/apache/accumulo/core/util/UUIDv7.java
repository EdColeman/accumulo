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

import java.security.SecureRandom;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate a time based UUID based on version 7.
 * <p>
 * See;
 * <a href="https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis">"rfc4122bis</a>
 */
public class UUIDv7 {

  private static final Logger LOG = LoggerFactory.getLogger(UUIDv7.class);
  private static final int field_ver = 0x7000; // 0x07;
  private static final byte field_var = 0x02;

  private static final int MAX_COUNT = 4096;

  private static final int NANO_BITS = 4096; // 12 bits.
  private static final double SCALE_NANO = NANO_BITS / 1_000_000_000.0; // max nano + 1.
  private Instant timestamp;
  private int counter = 0;

  private long overflowCounter = 0;
  private int highCount = 0;

  private final SecureRandom random = new SecureRandom();
  private final byte[] rand = new byte[8];

  public UUIDv7() {
    timestamp = Instant.now();
    random.nextBytes(rand);
  }

  public synchronized UUID next() {

    Instant now = Instant.now();

    if (now.toEpochMilli() > timestamp.toEpochMilli()) {
      counter = 0;
      timestamp = now;
      return generate();
    }

    if (now.toEpochMilli() == timestamp.toEpochMilli()) {
      counter++;
      highCount = Math.max(counter, highCount);
      if (counter >= MAX_COUNT) {
        counter = 0;
        overflowCounter++;
        // LOG.info("counter overflow, pausing for next system tick");
        timestamp = timestamp.plusMillis(1);
      }
    }
    return generate();
  }

  public long getOverflowCounter() {
    return overflowCounter;
  }

  public int getHighCount() {
    return highCount;
  }

  UUID generate() {
    long msb = timestamp.toEpochMilli() << 16;
    msb |= field_ver;
    msb |= counter;

    // LOG.info(String.format("%08x, %04x", timestamp.toEpochMilli(), counter));
    // LOG.info("nano: {}", timestamp.getNano());
    return new UUID(msb, 0);
  }

  /**
   * Scale the {@link Instant#getNano()} value to 12 bits. The max possible value for nanos is
   * 999_999_999, so this scaling is equivalent to (value / 1,000,000,000.0) * 4096, truncated to an
   * integer. The resolution of this scaling is 409 uSec.
   * <p>
   * For example:
   *
   * <pre>
   *     value = 999,999,999 (max) = (999,999,999 / 1,000,000,000.0) * 4096 = 4095.
   *     value = 409,600 (min) = (409,600 . 1,000,000,000.0) * 4096 = 1;
   * </pre>
   *
   * @param value nano portion of an Instant
   * @return value scaled to 12 bits resolution
   */
  int nanoScale12Bits(final int value) {
    return (int) (value * SCALE_NANO);
  }

  /**
   *
   * @return UUID formatted as hex string separated by hyphens in standard 8-4-4-4-12 format.
   */
  public String toString() {
    return "a";
  }

  public interface NanoClock {
    long nanoTime();
  }

  public class SystemNano implements NanoClock {
    @Override
    public long nanoTime() {
      return System.nanoTime();
    }
  }

  public class TestNano implements NanoClock {
    private final AtomicLong time;

    public TestNano(final AtomicLong source) {
      time = source;
    }

    @Override
    public long nanoTime() {
      return time.get();
    }

  }
}
