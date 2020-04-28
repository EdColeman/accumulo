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
package org.apache.accumulo.core.metrics;

import java.io.IOException;

import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

@AutoService(MetricsRegistration.class)
public class PrometheusMetricsRegistration implements MetricsRegistration {

  private static final Logger log = LoggerFactory.getLogger(PrometheusMetricsRegistration.class);

  public PrometheusMetricsRegistration() throws UnsupportedOperationException {
    log.warn("CONSTRUCTOR - PrometheusMetricsRegistration");
  }

  @Override
  public void register() throws UnsupportedOperationException {
    log.warn(
        "REGISTER - PrometheusMetricsRegistration metrics registration called - you should do something");

    try {
      ClassLoader loader = AccumuloClassLoader.getClassLoader();
      loader.loadClass("io.micrometer.prometheus.PrometheusMeterRegistry");
    } catch (IOException | ClassNotFoundException ex) {
      throw new UnsupportedOperationException("Registration failed", ex);
    }
    // search for micrometer-registry-X jars?

  }
}
