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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.spi.metrics.MeterRegistryFactory;
import org.apache.accumulo.server.ServiceMetrics;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

public class ServiceMetricsTest {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceMetrics.class);

  @Test
  public void metricsInitTest() throws Exception {

    MetricsServiceEnvironmentImpl env = createMock(MetricsServiceEnvironmentImpl.class);
    expect(env.isMicrometerEnabled()).andReturn(true).anyTimes();
    String factoryName = MetricsTestFactory.class.getCanonicalName();
    expect(env.getMetricsFactoryName()).andReturn(factoryName).anyTimes();
    expect(env.instantiate(anyString(), anyObject()))
        .andReturn(
            ConfigurationTypeHelper.getClassInstance(null, factoryName, MeterRegistryFactory.class))
        .anyTimes();
    expect(env.getCommonTags()).andReturn(List.of(Tag.of("tag1", "v1"))).anyTimes();
    expect(env.isJvmMetricsEnabled()).andReturn(true).anyTimes();

    replay(env);

    ServiceMetrics metrics = ServiceMetrics.getInstance(env);

    Counter c1 = Metrics.counter("counter-1");
    c1.increment();

    assertEquals(1.0, Metrics.globalRegistry.get("counter-1").counter().count());

    verify(env);
  }
}
