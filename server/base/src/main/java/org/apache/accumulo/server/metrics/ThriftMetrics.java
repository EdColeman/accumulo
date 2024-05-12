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
package org.apache.accumulo.server.metrics;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.annotations.MetricsDocProperty;
import org.apache.accumulo.annotations.VersionMapping;
import org.apache.accumulo.core.metrics.MetricsProducer;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

public class ThriftMetrics implements MetricsProducer {

  @MetricsDocProperty(name = "metrics init", description = "show things",
      versions = {@VersionMapping(version = "1.9.0", prevName = "old name 1"),
          @VersionMapping(version = "2.0.1", prevName = "another name")})
  private AtomicInteger counter = new AtomicInteger();

  @MetricsDocProperty(name = METRICS_THRIFT_IDLE, description = "show things",
      versions = {@VersionMapping(version = "1.9.0", prevName = "old name 1"),
          @VersionMapping(version = "2.0.1", prevName = "another name")})
  private DistributionSummary idle;

  @MetricsDocProperty(name = METRICS_THRIFT_EXECUTE, description = "show things",
      versions = {@VersionMapping(version = "1.9.0", prevName = "old name 1"),
          @VersionMapping(version = "2.0.1", prevName = "another name")})
  private DistributionSummary execute = new NoOpDistributionSummary();

  public ThriftMetrics() {
    idle = new NoOpDistributionSummary();
    counter.incrementAndGet();
  }

  public void addIdle(long time) {
    idle.record(time);
  }

  public void addExecute(long time) {
    execute.record(time);
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    idle = DistributionSummary.builder(METRICS_THRIFT_IDLE).baseUnit("ms").register(registry);
    execute = DistributionSummary.builder(METRICS_THRIFT_EXECUTE).baseUnit("ms").register(registry);
    counter =
        registry.gauge("thrift-counter", List.of(Tag.of("state", "tag")), new AtomicInteger(0));
  }

}
