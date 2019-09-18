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

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class Metrics2TestSink implements MetricsSink {

  OutputStream outStream = null;

  @Override public void putMetrics(MetricsRecord metricsRecord) {
    try {
      if (outStream != null) {
        for(AbstractMetric r : metricsRecord.metrics()) {
          String o = String.format("%s, %d\n", r.name(), r.value().longValue());
          outStream.write(o.getBytes());
        }
        outStream.write(String.format("%s\n", metricsRecord.toString()).getBytes());
        flush();
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  @Override public void flush() {
    try {
      outStream.flush();
    }catch (IOException ex){
      ex.printStackTrace();
    }
  }

  @Override public void init(SubsetConfiguration subsetConfiguration) {
    try {
      File f = new File("./target/mini-tests/metrics-test.out");
      outStream = new BufferedOutputStream(new FileOutputStream(f));
    } catch (Exception ex) {
      throw new IllegalStateException("Failed to init test metrics", ex);
    }
  }
}
