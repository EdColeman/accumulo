/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.functional.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metrics2TestSink implements MetricsSink, AutoCloseable {

  static final Logger log = LoggerFactory.getLogger(Metrics2TestSink.class);

  public static final byte[] NL_BYTES = "\n".getBytes(StandardCharsets.UTF_8);

  // private Metrics2SocketIpc.IpcSocketSource ipc = null;

  private String context = "";

  private AtomicReference<String> lastUpdate = new AtomicReference<>(new String("a"));

  private final Lock initLock = new ReentrantLock();
  private AtomicBoolean initialized = new AtomicBoolean(Boolean.FALSE);
  private MetricsTestServer server;

  @Override
  public void putMetrics(MetricsRecord metricsRecord) {
    try {

      String tmp = lastUpdate.get();
      lastUpdate.set(tmp + ",#");

      // JsonValues v = new JsonValues();
      // v.setTimestamp(metricsRecord.timestamp());
      // v.setContext(context);
      //
      // for (AbstractMetric r : metricsRecord.metrics()) {
      // v.addMetric(r.name(), Long.toString(r.value().longValue()));
      // }

      // v.sign();
      // ipc.send(v.toJson().getBytes());
      // Metrics2TestSink.lastUpdate.set(v);

      server.close();

    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public void flush() {

  }

  @Override
  public void init(SubsetConfiguration subsetConfiguration) {

    initLock.lock();
    try {

      if (initialized.get()) {
        return;
      }

      initialized.set(Boolean.TRUE);

      context = subsetConfiguration.getString("context");
      if (context == null || context.isEmpty()) {
        context = "metrics";
      } else if (context.startsWith(":")) {
        context = context.substring(1);
      }

      log.info("Initializing metrics reporting server with context \'{}\'", context);

      if (server == null) {
        server = new MetricsTestServer("metrics", lastUpdate);
      }

    } catch (Exception ex) {
      throw new IllegalStateException("Failed to initialize test metrics sink", ex);
    } finally {
      initLock.unlock();
    }
  }

  @Override
  public synchronized void close() throws IOException {

  }

}
