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

import static org.apache.hadoop.util.StringUtils.getTrimmedStrings;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metrics.MeterRegistryFactory;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;

public class MetricsInfoImpl implements MetricsInfo {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsInfoImpl.class);

  private final ServerContext context;

  private final Lock lock = new ReentrantLock();
  private final Map<String,Tag> processTags;

  private final Map<String,Tag> commonTags;

  private boolean metricsEnabled;

  private CompositeMeterRegistry composite = null;
  private final List<MeterRegistry> pendingRegistries = new ArrayList<>();

  private final List<MetricsProducer> producers = new ArrayList<>();

  public MetricsInfoImpl(final ServerContext context) {
    this.context = context;

    processTags = new HashMap<>();
    commonTags = new HashMap<>();
    Tag t = MetricsInfo.instanceNameTag(context.getInstanceName());
    commonTags.put(t.getKey(), t);
  }

  @Override
  public boolean metricsEnabled() {
    return true;
  }

  @Override
  public void addProcessTags(List<Tag> updates) {
    lock.lock();
    try {
      updates.forEach(t -> {
        if (!commonTags.containsKey(t.getKey())) {
          processTags.put(t.getKey(), t);
        }
      });
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void addCommonTags(List<Tag> updates) {
    lock.lock();
    try {
      if (composite != null) {
        LOG.warn(
            "Common tags after registry has been initialized may be ignored. Current common tags: {}",
            commonTags);
        return;
      }
      updates.forEach(t -> commonTags.put(t.getKey(), t));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Collection<Tag> getProcessTags() {
    lock.lock();
    try {
      return Collections.unmodifiableCollection(processTags.values());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Collection<Tag> getCommonTags() {
    lock.lock();
    try {
      return Collections.unmodifiableCollection(commonTags.values());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void addRegistry(MeterRegistry registry) {
    lock.lock();
    try {
      if (composite != null) {
        composite.add(registry);
      } else {
        // defer until composite is initialized
        pendingRegistries.add(registry);
      }

    } finally {
      lock.unlock();
    }
  }

  @Override
  public void addMetricsProducers(MetricsProducer... producer) {
    lock.lock();
    try {
      if (composite == null) {
        producers.addAll(Arrays.asList(producer));
      } else {
        Arrays.stream(producer).forEach(p -> p.registerMetrics(composite));
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public MeterRegistry getRegistry() {
    lock.lock();
    try {
      if (composite == null) {
        throw new IllegalStateException("metrics have not been initialized, call init() first");
      }
    } finally {
      lock.unlock();
    }
    return composite;
  }

  @Override
  public void init() {
    lock.lock();
    try {
      if (composite != null) {
        LOG.warn("metrics registry has already been initialized");
        return;
      }
      ArrayList<Tag> combinedTags = new ArrayList<>(commonTags.values());
      combinedTags.addAll(processTags.values());

      composite = new CompositeMeterRegistry();
      composite.config().commonTags(combinedTags);

      LOG.info("Metrics initialization. common tags: {}", combinedTags);

      new ClassLoaderMetrics(processTags.values()).bindTo(composite);
      new JvmMemoryMetrics(processTags.values()).bindTo(composite);
      var gc = new JvmGcMetrics(processTags.values());
      gc.bindTo(composite);
      new ProcessorMetrics(processTags.values()).bindTo(composite);
      new JvmThreadMetrics(processTags.values()).bindTo(composite);

      // user specified registries
      boolean enabled =
          context.getConfiguration().getBoolean(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED);
      String userRegistryFactories =
          context.getConfiguration().get(Property.GENERAL_MICROMETER_FACTORY);

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

      for (String factoryName : getTrimmedStrings(userRegistryFactories)) {
        try {
          MeterRegistry registry = getRegistryFromFactory(factoryName);
          registry.config().commonTags(commonTags.values());
          registry.config().commonTags(processTags.values());
          registry.config().meterFilter(replicationFilter);
          composite.add(registry);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException
            | InstantiationException | IllegalAccessException ex) {
          LOG.warn("Could not load registry " + factoryName, ex);
        }
      }

      Metrics.globalRegistry.add(composite);
      pendingRegistries.forEach(registry -> composite.add(registry));

      LOG.info("Metrics initialization. Register producers: {}", producers);
      producers.forEach(p -> p.registerMetrics(composite));

    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  @SuppressWarnings({"deprecation",
      "support for org.apache.accumulo.core.metrics.MeterRegistryFactory can be removed in 3.1"})
  static MeterRegistry getRegistryFromFactory(String factoryName)
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
      InstantiationException, IllegalAccessException {
    try {
      Class<? extends MeterRegistryFactory> clazz =
          ClassLoaderUtil.loadClass(factoryName, MeterRegistryFactory.class);
      MeterRegistryFactory factory = clazz.getDeclaredConstructor().newInstance();
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

  @Override
  public String toString() {
    return "MetricsCommonTags{tags=" + getProcessTags() + '}';
  }
}
