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
package org.apache.accumulo.server;

import java.util.List;

import org.apache.accumulo.core.spi.metrics.MeterRegistryFactory;
import org.apache.accumulo.server.metrics.MetricsServiceEnvironmentImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;

public class ServiceMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceMetrics.class);

  private final MetricsServiceEnvironmentImpl metricsEnv;

  private final boolean micrometerEnabled;
  private final String metricsFactoryName;

  private JvmGcMetrics gc = null;

  private static ServiceMetrics _instance = null;

  private ServiceMetrics(final MetricsServiceEnvironmentImpl env) {
    this.metricsEnv = env;

    micrometerEnabled = env.isMicrometerEnabled();
    metricsFactoryName = env.getMetricsFactoryName();

    LOG.info("initializing metrics. micrometer enabled: {}, factory name: {}", micrometerEnabled,
        metricsFactoryName);
    if (micrometerEnabled && !metricsFactoryName.isEmpty()) {
      try {
        MeterRegistryFactory factory =
            metricsEnv.instantiate(metricsFactoryName, MeterRegistryFactory.class);

        MeterRegistry registry = factory.create(metricsEnv);

        registry.config().commonTags(metricsEnv.getCommonTags());
        Metrics.addRegistry(registry);
      } catch (ReflectiveOperationException ex) {
        LOG.warn(
            "Metrics enabled, but unable to create metrics factory. Custom metrics will be unavailable",
            ex);
      }
    }
    LOG.info("initializing metrics. jvm metrics enabled: {}", env.isJvmMetricsEnabled());
    if (env.isJvmMetricsEnabled()) {
      initJvmMetrics();
    }
  }

  public static synchronized ServiceMetrics getInstance(final MetricsServiceEnvironmentImpl env) {
    if (_instance == null) {
      _instance = new ServiceMetrics(env);
    }
    return _instance;
  }

  void initJvmMetrics() {
    List<Tag> commonTags = metricsEnv.getCommonTags();
    new ClassLoaderMetrics(commonTags).bindTo(Metrics.globalRegistry);
    new JvmMemoryMetrics(commonTags).bindTo(Metrics.globalRegistry);
    gc = new JvmGcMetrics(commonTags);
    gc.bindTo(Metrics.globalRegistry);
    new ProcessorMetrics(commonTags).bindTo(Metrics.globalRegistry);
    new JvmThreadMetrics(commonTags).bindTo(Metrics.globalRegistry);
  }

  public void close() {
    if (gc != null) {
      gc.close();
    }
  }

  @Override
  public String toString() {
    return "MetricsInitUtil{micrometerEnabled=" + micrometerEnabled + ", metricsFactoryName='"
        + metricsFactoryName + '\'' + '}';
  }
}
