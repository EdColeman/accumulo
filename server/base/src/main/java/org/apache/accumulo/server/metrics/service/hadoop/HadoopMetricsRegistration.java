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
package org.apache.accumulo.server.metrics.service.hadoop;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.metrics.service.MetricsRegistrationService;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import io.micrometer.core.instrument.step.StepRegistryConfig;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;

@AutoService(MetricsRegistrationService.class)
public class HadoopMetricsRegistration implements MetricsRegistrationService {

  private static final Logger log = LoggerFactory.getLogger(HadoopMetricsRegistration.class);

  private final LoggingRegistryConfig lconf = c -> {
    if (c.equals("logging.step")) {
      return "5s";
    }
    return null;
  };

  @Override
  public void register(final ServerContext context, final String serviceName,
      final Map<String,String> properties, final CompositeMeterRegistry registry) {
    log.info("Loading metrics service name: {}, to {}, with props: {}", serviceName, registry,
        properties);
    if ("true".equals(properties.get("enabled"))) {
      log.info("enabled");
      registry.add(new HadoopMeterRegistry(HadoopRegistryConfig.DEFAULT, Clock.SYSTEM));
    }
  }

  public interface HadoopRegistryConfig extends StepRegistryConfig {

    HadoopRegistryConfig DEFAULT = k -> null;

    @Override
    default String prefix() {
      return "hadoop";
    }
  }

  public interface HadoopPollable {
    void poll();
  }
  public class HadoopMeterRegistry extends MeterRegistry {

    final Map<Meter.Id, HadoopPollable> meters = new HashMap<>();

    public HadoopMeterRegistry(HadoopRegistryConfig config, Clock clock) {
      this(config, HierarchicalNameMapper.DEFAULT, clock, null, null);
    }

    public HadoopMeterRegistry(HadoopRegistryConfig config, HierarchicalNameMapper nameMapper,
        Clock clock, Function<Meter.Id,HadoopLineBuilder> lineBuilderFunction,
        Consumer<String> lineSink) {
      super(clock);
      // start(new NamedThreadFactory("hadoop-metrics-publisher"));

      MetricsSystem ms = DefaultMetricsSystem.initialize("Accumulo");

    }

    @Override
    protected <T> Gauge newGauge(Meter.Id id, T t, ToDoubleFunction<T> toDoubleFunction) {
      return null;
    }

    @Override
    protected Counter newCounter(Meter.Id id) {
      return null;
    }

    @Override
    protected Timer newTimer(Meter.Id id, DistributionStatisticConfig distributionStatisticConfig,
        PauseDetector pauseDetector) {
      return null;
    }

    @Override
    protected DistributionSummary newDistributionSummary(Meter.Id id,
        DistributionStatisticConfig distributionStatisticConfig, double v) {
      return null;
    }

    @Override
    protected Meter newMeter(Meter.Id id, Meter.Type type, Iterable<Measurement> iterable) {
      return null;
    }

    @Override
    protected <T> FunctionTimer newFunctionTimer(Meter.Id id, T t, ToLongFunction<T> toLongFunction,
        ToDoubleFunction<T> toDoubleFunction, TimeUnit timeUnit) {
      return null;
    }

    @Override
    protected <T> FunctionCounter newFunctionCounter(Meter.Id id, T t,
        ToDoubleFunction<T> toDoubleFunction) {
      return null;
    }

    @Override
    protected TimeUnit getBaseTimeUnit() {
      return null;
    }

    @Override
    protected DistributionStatisticConfig defaultHistogramConfig() {
      return null;
    }
  }

  private interface HadoopLineBuilder {
    default String count(long amount) {
      return count(amount, Statistic.COUNT);
    }

    String count(long amount, Statistic stat);

    default String gauge(double amount) {
      return gauge(amount, Statistic.VALUE);
    }

    String gauge(double amount, Statistic stat);

    String histogram(double amount);

    String timing(double timeMs);
  }
}
