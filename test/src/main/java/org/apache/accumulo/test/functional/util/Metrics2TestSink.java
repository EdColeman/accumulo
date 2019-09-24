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
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class Metrics2TestSink implements MetricsSink, AutoCloseable {

  static final Logger log = LoggerFactory.getLogger(Metrics2TestSink.class);

  public static final byte[] NL_BYTES = "\n".getBytes(StandardCharsets.UTF_8);

  // private Metrics2SocketIpc.IpcSocketSource ipc = null;

  private String context = "";

  private AtomicReference<String> lastUpdate = new AtomicReference<>(new String("a"));

  private final Lock initLock = new ReentrantLock();
  private AtomicBoolean initialized = new AtomicBoolean(Boolean.FALSE);
  private MetricsServer server;

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
        server = new MetricsServer("metrics", lastUpdate);
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

  private static class MetricsServer implements Runnable {

    private HttpServer server;
    private AtomicReference<String> updates;

    MetricsServer(final String context, final AtomicReference<String> lastUpdate) {
      updates = lastUpdate;
      try {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLocalHost(), 12332), 0);
        server.createContext("/" + context, new MyHandler(updates));
        server.setExecutor(null); // creates a default executor
        server.start();

        log.error("Listen on {}", server.getAddress());

      } catch (IOException ex) {
        log.debug("Failed to create metrics server for test sink - requests not available", ex);
        server = null;
      }
    }

    public void close() {
      server.stop(0);
    }

    static class MyHandler implements HttpHandler {

      private AtomicReference<String> update;

      public MyHandler(AtomicReference<String> update) {
        this.update = update;
      }

      @Override
      public void handle(HttpExchange t) throws IOException {
        // JsonValues v = Metrics2TestSink.lastUpdate.get();
        String v = update.get();

        if (v == null) {
          t.sendResponseHeaders(204, -1);
        } else {
          // String response = v.toJson();
          String response = v;
          t.sendResponseHeaders(200, response.getBytes(StandardCharsets.UTF_8).length);
          OutputStream os = t.getResponseBody();
          os.write(response.getBytes(StandardCharsets.UTF_8));
          os.close();
        }
      }
    }

    @Override
    public void run() {

    }
  }

}
