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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generate a time based UUID based on version 7.
 * <p>
 * See;
 * <a href="https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis">"rfc4122bis</a>
 */
public class UUIDv7 {

  private static final int ver = 0x07000000; // 0x07;
  private static final byte var = 0x02;

  private static final int MAX_COUNT = 4096;
  private volatile Instant timestamp = null;
  private AtomicInteger counter = new AtomicInteger();
  private byte[] rand = new byte[8];
  public UUIDv7(){
    SecureRandom random = new SecureRandom();
    random.nextBytes(rand);
    timestamp = Instant.now();
    timestamp.getEpochSecond();
  }

  public UUID next() {
    byte[] buffer = new byte[16];

    int offset = 0;
    long time = timestamp.getEpochSecond();
    return null;
  }

  /**
   *
   * @return UUID formatted as hex string separated by hyphens in standard 8-4-4-4-12 format.
   */
  public String toString(){
    return "a";
  }
}
