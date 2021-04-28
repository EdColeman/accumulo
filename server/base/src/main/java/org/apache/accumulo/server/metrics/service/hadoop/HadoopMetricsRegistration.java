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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.step.StepRegistryConfig;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.metrics.service.MetricsRegistrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;

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

    @Override default String prefix(){
      return "hadoop";
    }
  }

  public class HadoopMeterRegistry extends StepMeterRegistry {

    public HadoopMeterRegistry(HadoopRegistryConfig config, Clock clock) {
      super(config, clock);

      start(new NamedThreadFactory("custom-metrics-publisher"));
    }

    @Override protected void publish() {
      getMeters().stream().forEach(meter -> System.out.println("Publishing " + meter.getId() + ":" + meter.toString()));
    }

    @Override protected TimeUnit getBaseTimeUnit() {
      return TimeUnit.MILLISECONDS;
    }
  }
}
