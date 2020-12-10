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

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class PropCache {

  private static final Logger log = LoggerFactory.getLogger(PropCache.class);

  private final LoadingCache<String,PropEncoding> cache =
      CacheBuilder.newBuilder().build(new CacheLoader<String,PropEncoding>() {
        @Override
        public PropEncoding load(String s) throws Exception {
          return readFromStore(s);
        }
      });

  private PropEncoding readFromStore(String tableId) {
    return new PropEncodingV1(1, true, Instant.now());
  }
}
