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
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.MetricsVisitor;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class Metrics2TestSinkTest {
  private static final Logger log = LoggerFactory.getLogger(Metrics2TestSinkTest.class);

  @Test public void jsonRoundTrip() {
    Map<String,String> expected = new TreeMap<>();
    expected.put("a", "1");
    expected.put("b", "2");
    Metrics2TestSink.JsonValues serdes = new Metrics2TestSink.JsonValues();

    serdes.setTimestamp(System.currentTimeMillis());
    for (Map.Entry<String,String> e : expected.entrySet()) {
      serdes.addMetric(e.getKey(), e.getValue());
    }
    serdes.sign();
    String json = serdes.toJson();

    log.info("Payload: {}", json);

    Gson gson = new GsonBuilder().create();
    Metrics2TestSink.JsonValues r = Metrics2TestSink.JsonValues.fromJson(json);

    assertEquals(serdes.getTimestamp(), r.getTimestamp());
    assertEquals(serdes.getSignature(), r.getSignature());
    assertEquals(expected, r.getMetrics());
  }

  @Test public void create() throws Exception {

    Metrics2TestSink metrics = new Metrics2TestSink();

    Configuration config = new BaseConfiguration();
    config.addProperty("metrics.context", "accumulo.gc");

    SubsetConfiguration subsetConfiguration = (SubsetConfiguration) config.subset("metrics");

    metrics.init(subsetConfiguration);

    MetricsClient client = new MetricsClient();

    log.info("X: {}", client.getMetrics());

    MetricsCollector collector = new MetricsCollectorImpl();

     final String CONTEXT = "accumulo.gc";
     final String RECORD = "accumulo_gc_run_stats";

      metrics.putMetrics(new FakeRecord());

    log.info("X: {}", client.getMetrics());

  }

  static class FakeMetric extends AbstractMetric {

    protected FakeMetric(MetricsInfo info) {
      super(info);
    }

    @Override public Number value() {
      return 99;
    }

    @Override public MetricType type() {
      return MetricType.GAUGE;
    }

    @Override public void visit(MetricsVisitor metricsVisitor) {

    }
  }
  static class FakeRecord implements MetricsRecord{

    Map<String,Long> m = new TreeMap<>();

    public FakeRecord(){

    }
    @Override public long timestamp() {
      return System.currentTimeMillis();
    }

    @Override public String name() {
      return "aName";
    }

    @Override public String description() {
      return "a description";
    }

    @Override public String context() {
      return "context";
    }

    @Override public Collection<MetricsTag> tags() {
      return null;
    }

    @Override public Iterable<AbstractMetric> metrics() {
      return null;
    }
  }

}
