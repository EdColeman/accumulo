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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class SentinelRoot {

  private static final Logger log = LoggerFactory.getLogger(SentinelRoot.class);

  private static final int NUM_SENTINEL_NODES = 16;
  private static final int MASK = NUM_SENTINEL_NODES - 1;

  private static final HashFunction hasher = Hashing.murmur3_32();

  private final String[] nodes = new String[NUM_SENTINEL_NODES];

  public int lookup(final String id) {
    int bin = hasher.hashString(id, UTF_8).hashCode() & MASK;
    return bin;
  }
}
