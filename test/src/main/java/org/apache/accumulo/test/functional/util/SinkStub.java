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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkStub implements MetricsSink, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(SinkStub.class);

  private final AtomicBoolean initialized = new AtomicBoolean(Boolean.FALSE);

  private PrintWriter pw = null;

  private AtomicReference<JsonMetricsValues> lastUpdate = new AtomicReference<>(null);

  private AtomicReference<String> stub = new AtomicReference<>(null);

  @Override
  public void putMetrics(MetricsRecord metricsRecord) {

    JsonMetricsValues metrics = new JsonMetricsValues();
    metrics.setTimestamp(System.currentTimeMillis());

    for (AbstractMetric r : metricsRecord.metrics()) {
      metrics.addMetric(r.name(), Long.toString(r.value().longValue()));
    }

    metrics.sign();

    lastUpdate.set(metrics);

    publish("", lastUpdate);

    stub.set(lastUpdate.get().toJson());

    pw.println(String.format("%s", lastUpdate.get().toJson()));
    pw.flush();
  }

  @Override
  public void flush() {
    if (pw != null) {
      pw.flush();
    }
  }

  public synchronized void close() {
    if (pw != null) {
      pw.flush();
      PrintWriter tmp = pw;
      pw = null;
      tmp.close();
    }
  }

  @Override
  public synchronized void init(SubsetConfiguration subsetConfiguration) {

    if (initialized.get()) {
      return;
    }

    try {
      FileWriter fw = new FileWriter("/tmp/test-sink.txt"); // this erases previous content
      fw.close();
      // this reopens file for appending
      pw = new PrintWriter(new BufferedWriter(new FileWriter("/tmp/test-sink.txt", true)));
    } catch (IOException ex) {
      ex.printStackTrace();
      if (pw != null) {
        pw.close();
        pw = null;
      }

      return;
    }

    initialized.set(Boolean.TRUE);

  }

  private void publish(final String context, final AtomicReference<JsonMetricsValues> metricsRef){

    try(CloseableHttpClient httpclient = HttpClients.createDefault()) {

      final String postUri = String
          .format("http://%s:%d/post", InetAddress.getLocalHost().getHostAddress(), 12332);

      log.debug("post destination uri: \'{}\'", postUri);

      StringEntity entity = new StringEntity(metricsRef.get().toJson());
      HttpPost httpPost = new HttpPost(postUri);

      httpPost.setEntity(entity);
      httpPost.setHeader("Accept", "application/json");
      httpPost.setHeader("Content-type", "application/json");

      CloseableHttpResponse response = httpclient.execute(httpPost);

      int status = response.getStatusLine().getStatusCode();

      if (status >= 200 && status < 300) {
        log.debug("metrics post successful");
      } else {
        log.debug("metrics post failed with status {}", status);
      }

    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}
