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
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

public class PostRx {

  private static final Logger log = LoggerFactory.getLogger(Metrics2TestSink.class);

  private final HttpServer server;
  private final AtomicReference<JsonMetricsValues> metrics = new AtomicReference<>();

  public PostRx() throws Exception {

    String context = "post";

    server = HttpServer.create(new InetSocketAddress(InetAddress.getLocalHost(), 12332), 0);
    server.createContext("/" + context, new MyHandler(metrics));
    server.setExecutor(null); // creates a default executor
    server.start();

  }

  public JsonMetricsValues getMetrics(){
    return metrics.get();
  }

  static class MyHandler implements HttpHandler {

    private AtomicReference<JsonMetricsValues> update;

    public MyHandler(AtomicReference<JsonMetricsValues> update) {
      this.update = update;
    }

    @Override public void handle(HttpExchange t) throws IOException {
      log.info("Received H: {}", t.getRequestHeaders().entrySet());
      log.info("Received M: {}", t.getRequestMethod());

      log.info("C: {}", t.getHttpContext());

      InputStream is = t.getRequestBody();

      String msgContents = IOUtils.toString(is);
      is.close();

      update.set(JsonMetricsValues.fromJson(msgContents));

      t.sendResponseHeaders(200, -1);
      //      OutputStream os = t.getResponseBody();
      //      os.write("200".getBytes(StandardCharsets.UTF_8));
      //      os.close();
    }
  }
}
