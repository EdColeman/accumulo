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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

class MetricsTestServer implements Runnable {

  static final Logger log = LoggerFactory.getLogger(MetricsTestServer.class);

  private HttpServer server;
  private AtomicReference<String> updates;

  MetricsTestServer(final String context, final AtomicReference<String> lastUpdate) {
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
