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

import java.util.Map;

import org.apache.accumulo.core.spi.common.ServiceEnvironment;

import com.google.common.base.Preconditions;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * The Micrometer metrics allows for different monitoring systems. and can be enabled within
 * Accumulo with properties and are initialized by implementing this interface and providing the
 * factory implementation clas name as a property. Metrics are specified with the following
 * properties:
 * <p>
 * Property.GENERAL_MICROMETER_ENABLED = true
 * <p>
 * Property.GENERAL_MICROMETER_FACTORY = [implementation].class.getName()
 */
public interface MeterRegistryFactory {
  // full form in property file is "general.custom.metrics.opts"
  String METRICS_PROP_SUBSTRING = "metrics.opts";

  interface InitParameters {
    /**
     *
     * @return The configured options. For example properties
     *         {@code general.custom.metrics.opts.prop1=abc} and
     *         {@code general.custom.metrics.opts.prop9=123} were set, then this map would contain
     *         {@code prop1=abc} and {@code prop9=123}.
     */
    Map<String,String> getOptions();

    ServiceEnvironment getServiceEnv();
  }

  default void init(InitParameters params) {
    Preconditions.checkArgument(params.getOptions().isEmpty(), "No options expected");
  }

  /**
   * Called on metrics initialization.
   *
   * @return a Micrometer registry that will be added to the metrics configuration.
   */
  MeterRegistry create();
}
