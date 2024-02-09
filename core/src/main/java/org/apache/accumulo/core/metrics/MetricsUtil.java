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
package org.apache.accumulo.core.metrics;

import static org.apache.accumulo.core.conf.Property.GENERAL_ARBITRARY_PROP_PREFIX;
import static org.apache.accumulo.core.spi.metrics.MeterRegistryFactory.METRICS_PROP_SUBSTRING;
import static org.apache.hadoop.util.StringUtils.getTrimmedStrings;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.metrics.MeterRegistryFactory;
import org.apache.accumulo.core.util.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;

public class MetricsUtil {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsUtil.class);

  private static JvmGcMetrics gc;
  private static List<Tag> commonTags;

  public static void initializeMetrics(final AccumuloConfiguration conf, final String appName,
      final HostAndPort address, final String instanceName) throws ClassNotFoundException,
      InstantiationException, IllegalAccessException, IllegalArgumentException,
      IllegalStateException, InvocationTargetException, NoSuchMethodException, SecurityException {

    final boolean enabled = conf.getBoolean(Property.GENERAL_MICROMETER_ENABLED);
    final boolean jvmMetricsEnabled =
        conf.getBoolean(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED);
    final String factoryClasses = conf.get(Property.GENERAL_MICROMETER_FACTORY);

    // todo - start
    Map<String,
        String> opts = conf.getAllPropertiesWithPrefixStripped(GENERAL_ARBITRARY_PROP_PREFIX)
            .entrySet().stream().filter(entry -> entry.getKey().startsWith(METRICS_PROP_SUBSTRING))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    MeterRegistryFactory.InitParameters initParameters = new MeterRegistryFactory.InitParameters() {

      private final ServiceEnvironment senv = null; // new ServiceEnvironmentImpl(context);

      @Override
      public Map<String,String> getOptions() {
        return opts;
      }

      @Override
      public ServiceEnvironment getServiceEnv() {
        return senv;
      }
    };

    // todo - end
    LOG.info("initializing metrics, enabled:{}, class:{}", enabled, factoryClasses);

    if (enabled && factoryClasses != null && !factoryClasses.isEmpty()) {

      String processName = appName;
      String serviceInstance = System.getProperty("accumulo.metrics.service.instance", "");
      if (!serviceInstance.isBlank()) {
        processName += serviceInstance;
      }

      List<Tag> tags = new ArrayList<>();
      tags.add(Tag.of("instance.name", instanceName));
      tags.add(Tag.of("process.name", processName));

      if (address != null) {
        if (!address.getHost().isEmpty()) {
          tags.add(Tag.of("host", address.getHost()));
        }
        if (address.getPort() > 0) {
          tags.add(Tag.of("port", Integer.toString(address.getPort())));
        }
      }

      commonTags = Collections.unmodifiableList(tags);

      // Configure replication metrics to display percentiles and change its expiry to 10 mins
      MeterFilter replicationFilter = new MeterFilter() {
        @Override
        public DistributionStatisticConfig configure(Meter.Id id,
            DistributionStatisticConfig config) {
          if (id.getName().equals("replicationQueue")) {
            return DistributionStatisticConfig.builder().percentiles(0.5, 0.75, 0.9, 0.95, 0.99)
                .expiry(Duration.ofMinutes(10)).build().merge(config);
          }
          return config;
        }
      };

      for (String factoryName : getTrimmedStrings(factoryClasses)) {
        MeterRegistry registry = getRegistryFromFactory(factoryName, initParameters);
        registry.config().commonTags(commonTags);
        registry.config().meterFilter(replicationFilter);
        Metrics.addRegistry(registry);
      }

      if (jvmMetricsEnabled) {
        new ClassLoaderMetrics(commonTags).bindTo(Metrics.globalRegistry);
        new JvmMemoryMetrics(commonTags).bindTo(Metrics.globalRegistry);
        gc = new JvmGcMetrics(commonTags);
        gc.bindTo(Metrics.globalRegistry);
        new ProcessorMetrics(commonTags).bindTo(Metrics.globalRegistry);
        new JvmThreadMetrics(commonTags).bindTo(Metrics.globalRegistry);
      }
    }
  }

  @VisibleForTesting
  @SuppressWarnings({"deprecation",
      "support for org.apache.accumulo.core.metrics.MeterRegistryFactory can be removed in 3.1"})
  static MeterRegistry getRegistryFromFactory(String factoryName,
      final MeterRegistryFactory.InitParameters initParameters)
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
      InstantiationException, IllegalAccessException {
    try {
      Class<? extends MeterRegistryFactory> clazz =
          ClassLoaderUtil.loadClass(factoryName, MeterRegistryFactory.class);
      MeterRegistryFactory factory = clazz.getDeclaredConstructor().newInstance();
      factory.init(initParameters);
      return factory.create();
    } catch (ClassCastException ex) {
      // empty. On exception try deprecated version
    }
    try {
      Class<? extends org.apache.accumulo.core.metrics.MeterRegistryFactory> clazz = ClassLoaderUtil
          .loadClass(factoryName, org.apache.accumulo.core.metrics.MeterRegistryFactory.class);
      org.apache.accumulo.core.metrics.MeterRegistryFactory factory =
          clazz.getDeclaredConstructor().newInstance();
      return factory.create();
    } catch (ClassCastException ex) {
      // empty. No valid metrics factory, fall through and then throw exception.
    }
    throw new ClassNotFoundException(
        "Could not find appropriate class implementing a MetricsFactory for: " + factoryName);
  }

  public static void initializeProducers(MetricsProducer... producer) {
    for (MetricsProducer p : producer) {
      p.registerMetrics(Metrics.globalRegistry);
      LOG.info("Metric producer {} initialize", p.getClass().getSimpleName());
    }
  }

  public static void addExecutorServiceMetrics(ExecutorService executor, String name) {
    new ExecutorServiceMetrics(executor, name, commonTags).bindTo(Metrics.globalRegistry);
  }

  public static List<Tag> getCommonTags() {
    return commonTags;
  }

  public static void close() {
    if (gc != null) {
      gc.close();
    }
  }

}
