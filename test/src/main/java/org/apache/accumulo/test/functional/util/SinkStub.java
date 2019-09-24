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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;

public class SinkStub implements MetricsSink, AutoCloseable {

  private final AtomicBoolean initialized = new AtomicBoolean(Boolean.FALSE);

  private PrintWriter pw = null;

  private AtomicReference<JsonMetricsValues> lastUpdate = new AtomicReference<>(null);

  @Override
  public void putMetrics(MetricsRecord metricsRecord) {

    JsonMetricsValues metrics = new JsonMetricsValues();
    metrics.setTimestamp(System.currentTimeMillis());

    for (AbstractMetric r : metricsRecord.metrics()) {
      metrics.addMetric(r.name(), Long.toString(r.value().longValue()));
    }

    metrics.sign();

    lastUpdate.set(metrics);

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
}
