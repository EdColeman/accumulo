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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.server.ServerContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class MetricsInfoImplTest {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsInfoImplTest.class);

  private ServerContext context;

  private final LoggingRegistryConfig lconf = c -> {
    if (c.equals("logging.step")) {
      return "10s";
    }
    return null;
  };

  @BeforeEach
  public void init() {
    context = createMock(ServerContext.class);
    expect(context.getInstanceName()).andReturn("test-instance").anyTimes();
    SiteConfiguration aconf = SiteConfiguration.empty()
        .withOverrides(Map.of(Property.GENERAL_MICROMETER_FACTORY.getKey(), "bob")).build();
    ConfigurationCopy config = new ConfigurationCopy(aconf);
    expect(context.getConfiguration()).andReturn(config).anyTimes();
    replay(context);
  }

  @AfterEach
  public void verifyMocks() {
    verify(context);
  }

  @Test
  public void registryTest() {
    List<String> metricsArray = new ArrayList<>();

    MetricsInfo mi = new MetricsInfoImpl(context);
    Tag sys = Tag.of("system", "test1");
    Tag app = Tag.of("app", "app1");
    Tag port = Tag.of("port", "8080");

    mi.addCommonTags(List.of(sys, app, port));

    mi.init();
    final Consumer<String> logConsumer = LOG::info;
    Consumer<String> arrayConsumer = metricsArray::add;

    LoggingMeterRegistry loggingRegistry =
        LoggingMeterRegistry.builder(lconf).loggingSink(logConsumer).build();

    LoggingMeterRegistry arrayLoggingRegistry =
        LoggingMeterRegistry.builder(lconf).loggingSink(arrayConsumer).build();

    MeterRegistry r1 = new SimpleMeterRegistry();
    mi.addRegistry(r1);
    mi.addRegistry(arrayLoggingRegistry);
    mi.addRegistry(loggingRegistry);

    Counter c1 = mi.getRegistry().counter("counter1");

    LOG.info("meters: {}", mi.getRegistry().getMeters().get(0).measure());
    var v = mi.getRegistry().getMeters().get(0).measure();

    LOG.info("config: {}, value: {}", mi.getRegistry().config().commonTags(), v);

    try {
      for (int i = 0; i < 2; i++) {
        c1.increment();
        Thread.sleep(10_000);
        LOG.info("A: {}", metricsArray);
        r1.getMeters().forEach(m -> LOG.info("M: {}", m.measure()));
      }
    } catch (Exception ex) {
      // ignore
    }

    mi.getRegistry().config().commonTags(List.of(Tag.of("dynamic", "COMPOSITE-1")));
    Metrics.globalRegistry.config().commonTags(List.of(Tag.of("dynamic", "GLOBAL-1")));
    Counter c2 = mi.getRegistry().counter("counter2");

    try {
      for (int i = 0; i < 2; i++) {
        c1.increment();
        c2.increment();
        Thread.sleep(10_000);
        metricsArray.forEach(m -> LOG.info("A: {}", m));
        r1.getMeters().forEach(m -> LOG.info("M: {}", m.measure()));
        metricsArray.clear();
      }
    } catch (Exception ex) {
      // ignore
    }

  }

  @Test
  public void x() {
    LOG.info("tag:{}", Tag.of("a", "b"));
    LOG.info("tag:{}", Tag.of("host", ""));
  }
}
