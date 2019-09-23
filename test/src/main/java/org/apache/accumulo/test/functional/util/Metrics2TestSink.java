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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metrics2TestSink implements MetricsSink, AutoCloseable {

  static final Logger log = LoggerFactory.getLogger(Metrics2TestSink.class);

  public static final byte[] NL_BYTES = "\n".getBytes(StandardCharsets.UTF_8);

  private Metrics2SocketIpc.IpcSocketSource ipc = null;

  private String context = "";

  @Override
  public void putMetrics(MetricsRecord metricsRecord) {
    try {

      JsonValues v = new JsonValues();
      v.setTimestamp(metricsRecord.timestamp());
      v.setContext(context);

      for (AbstractMetric r : metricsRecord.metrics()) {
        v.addMetric(r.name(), Long.toString(r.value().longValue()));
      }

      v.sign();
      ipc.send(v.toJson().getBytes());

    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Override
  public void flush() {

  }

  @Override
  public synchronized void init(SubsetConfiguration subsetConfiguration) {

    try {

      if (ipc != null) {
        return;
      }
      
      context = subsetConfiguration.getString("context");
      if (context.startsWith(":")) {
        context = context.substring(1);
      }

      ipc = new Metrics2SocketIpc.IpcSocketSource();

    } catch (Exception ex) {
      throw new IllegalStateException("Failed to init test metrics", ex);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (ipc != null) {
      ipc.close();
    }
    ipc = null;
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

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("JsonValues{");
      sb.append("timestamp=").append(timestamp);
      sb.append(", metrics=").append(metrics);
      sb.append(", signature='").append(signature).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }
}
