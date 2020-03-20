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
package org.apache.accumulo.master.metrics;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import org.apache.accumulo.server.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;

import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

public abstract class MasterMetrics extends Metrics {

  private final static Logger log = LoggerFactory.getLogger(MasterMetrics.class);

  private static HttpServer server = null;

  protected MasterMetrics(String subName, String description, String record) {
    super("Master,sub=" + subName, description, "master", record);

    io.micrometer.core.instrument.Metrics.addRegistry(new SimpleMeterRegistry());

    io.micrometer.core.instrument.Metrics.addRegistry(new LoggingMeterRegistry());

    PrometheusMeterRegistry prometheusRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    io.micrometer.core.instrument.Metrics.addRegistry(prometheusRegistry);
    //
    sampleHttpServer(prometheusRegistry);
  }

  private synchronized static void
      sampleHttpServer(final PrometheusMeterRegistry prometheusRegistry) {
    if (server != null) {
      return;
    }

    try {
      server = HttpServer.create(new InetSocketAddress(35353), 0);
      server.createContext("/prometheus", httpExchange -> {
        String response = prometheusRegistry.scrape();

        httpExchange.sendResponseHeaders(200, response.getBytes().length);
        try (OutputStream os = httpExchange.getResponseBody()) {
          os.write(response.getBytes());
        }
      });

      new Thread(server::start).start();

      log.info("Http server started for /prometheus end-point");

    } catch (IOException e) {
      throw new RuntimeException("Exception starting prometheus http endpoint", e);
    }
  }
}
