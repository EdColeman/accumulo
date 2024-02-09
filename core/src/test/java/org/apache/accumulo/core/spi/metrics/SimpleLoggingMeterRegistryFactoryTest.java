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
package org.apache.accumulo.core.spi.metrics;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Map;

import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;

class SimpleLoggingMeterRegistryFactoryTest {

  @Test
  public void createTest() {
    SimpleLoggingMeterRegistryFactory factory = new SimpleLoggingMeterRegistryFactory();
    factory.init(new LoggingMetricsParams());
    assertInstanceOf(MeterRegistry.class, factory.create());
  }

  private static class LoggingMetricsParams implements MeterRegistryFactory.InitParameters {

    @Override
    public Map<String,String> getOptions() {
      return Map.of();
    }

    @Override
    public ServiceEnvironment getServiceEnv() {
      return null;
    }
  }
}
