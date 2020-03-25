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
package org.apache.accumulo.server.metrics.registry;

import io.micrometer.core.instrument.step.StepRegistryConfig;

public interface MetricsFileRegistryConfig extends StepRegistryConfig {

  MetricsFileRegistryConfig DEFAULT = k -> null;

  @Override
  default String prefix() {
    return "file";
  }

  /**
   * @return Whether counters and timers that have no activity in an interval are still logged.
   */
  default boolean logInactive() {
    String v = get(prefix() + ".logInactive");
    return Boolean.parseBoolean(v);
  }

  default String filePath() {
    String v = get(prefix() + ".filePath");
    if (v == null || v.isEmpty()) {
      return "/tmp/all.metrics.out";
    }
    return v;
  }

}
