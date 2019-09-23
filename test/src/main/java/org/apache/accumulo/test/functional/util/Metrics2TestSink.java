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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Metrics2TestSink implements MetricsSink, AutoCloseable {

  static final Logger log = LoggerFactory.getLogger(Metrics2TestSink.class);

  public static final byte[] NL_BYTES = "\n".getBytes(StandardCharsets.UTF_8);

  // private Metrics2SocketIpc.IpcSocketSource ipc = null;

  private String context = "";

  private AtomicReference<String> lastUpdate = new AtomicReference<>(new String("a"));

  private final Lock initLock = new ReentrantLock();
  private AtomicBoolean initialized = new AtomicBoolean(Boolean.FALSE);
  private MetricsServer server;

  @Override public void putMetrics(MetricsRecord metricsRecord) {
    try {

      String tmp = lastUpdate.get();
      lastUpdate.set(tmp+",#");

//      JsonValues v = new JsonValues();
//      v.setTimestamp(metricsRecord.timestamp());
//      v.setContext(context);
//
//      for (AbstractMetric r : metricsRecord.metrics()) {
//        v.addMetric(r.name(), Long.toString(r.value().longValue()));
//      }

//      v.sign();
      // ipc.send(v.toJson().getBytes());
      // Metrics2TestSink.lastUpdate.set(v);

      server.close();

    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Override public void flush() {

  }

  @Override public void init(SubsetConfiguration subsetConfiguration) {

    initLock.lock();
    try {

      if(initialized.get()){
        return;
      }

      initialized.set(Boolean.TRUE);

      context = subsetConfiguration.getString("context");
      if(context == null || context.isEmpty()){
        context = "metrics";
      }else if (context.startsWith(":")) {
        context = context.substring(1);
      }

      log.info("Initializing metrics reporting server with context \'{}\'", context);

      if(server == null){
        server = new MetricsServer("metrics", lastUpdate);
      }

    } catch (Exception ex) {
      throw new IllegalStateException("Failed to initialize test metrics sink", ex);
    }finally {
      initLock.unlock();
    }
  }

  @Override public synchronized void close() throws IOException {

  }

  private static class MetricsServer implements Runnable {

    private HttpServer server;
    private AtomicReference<String> updates;

    MetricsServer(final String context, final AtomicReference<String> lastUpdate) {
      updates = lastUpdate;
      try {
        server = HttpServer
            .create(new InetSocketAddress(InetAddress.getLocalHost(), 12332), 0);
        server.createContext("/" + context, new MyHandler(updates));
        server.setExecutor(null); // creates a default executor
        server.start();

        log.error("Listen on {}", server.getAddress());

      } catch (IOException ex) {
        log.debug("Failed to create metrics server for test sink - requests not available", ex);
        server = null;
      }
    }

    public void close(){
      server.stop(0);
    }

    static class MyHandler implements HttpHandler {

      private AtomicReference<String> update;

      public MyHandler(AtomicReference<String> update){
        this.update = update;
      }

      @Override public void handle(HttpExchange t) throws IOException {
        //JsonValues v = Metrics2TestSink.lastUpdate.get();
        String v = update.get();

        if(v == null){
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

    @Override public void run() {

    }
  }

  public static class JsonValues {

    private long timestamp;
    private String context;
    private Map<String,String> metrics;
    private String signature;

    public JsonValues() {
      metrics = new TreeMap<>();
    }

    public long getTimestamp() {
      return timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }

    public String getContext() {
      return context;
    }

    public void setContext(String context) {
      this.context = context;
    }

    public Map<String,String> getMetrics() {
      return metrics;
    }

    public void addMetric(final String name, final String value) {
      metrics.put(name, value);
    }

    public String getSignature() {
      return signature;
    }

    public void sign() {
      MessageDigest digest;
      try {
        digest = MessageDigest.getInstance("SHA-256");
      } catch (NoSuchAlgorithmException ex) {
        signature = "-1";
        return;
      }
      digest.reset();
      digest.update(Long.toString(timestamp).getBytes());
      for (Map.Entry<String,String> e : metrics.entrySet()) {
        digest.update(e.getKey().getBytes());
        digest.update(e.getValue().getBytes());
      }

      byte[] md = digest.digest();

      long x = 0;
      for (int i = 0; i < 8; i++) {
        x |= (0xff & (long) md[i]) << i * 8;
      }
      signature = Long.toHexString(x);
    }

    public String toJson() {
      Gson gson = new GsonBuilder().create();
      return gson.toJson(this);
    }

    public static JsonValues fromJson(final String json) {
      Gson gson = new GsonBuilder().create();
      return gson.fromJson(json, Metrics2TestSink.JsonValues.class);
    }

    public static JsonValues fromJson(final InputStream in) {

      try {
        Gson gson = new GsonBuilder().create();
        return gson.fromJson(IOUtils.toString(in), Metrics2TestSink.JsonValues.class);
      }catch(IOException ex){
        log.info("Failed to process input stream");
      }
      return null;
    }

    @Override public String toString() {
      final StringBuilder sb = new StringBuilder("JsonValues{");
      sb.append("timestamp=").append(timestamp);
      sb.append(", metrics=").append(metrics);
      sb.append(", signature='").append(signature).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }
}
