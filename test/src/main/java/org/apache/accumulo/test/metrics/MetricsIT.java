/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.metrics;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.metrics.LoggingMeterRegistryFactory;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.metrics.TestStatsDSink.Metric;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;

public class MetricsIT extends ConfigurableMacBase implements MetricsProducer {

  private static TestStatsDSink sink;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @BeforeAll
  public static void before() throws Exception {
    sink = new TestStatsDSink();
  }

  @AfterAll
  public static void after() throws Exception {
    sink.close();
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(2);
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.MANAGER_FATE_METRICS_MIN_UPDATE_INTERVAL, "1s");
    // Tell the server processes to use a StatsDMeterRegistry and the simple logging registry
    // that will be configured to push all metrics to the sink we started.
    cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED, "true");
    cfg.setProperty("general.custom.metrics.opts.logging.step", "1s");
    String clazzList = LoggingMeterRegistryFactory.class.getName() + ","
        + TestStatsDRegistryFactory.class.getName();
    cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY, clazzList);
    Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
        TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
    cfg.setSystemProperties(sysProps);
  }

  @Test
  public void confirmMetricsPublished() throws Exception {

    doWorkToGenerateMetrics();
    cluster.stop();

    Set<String> unexpectedMetrics = Set.of(METRICS_SCAN_YIELDS, METRICS_UPDATE_ERRORS,
        METRICS_REPLICATION_QUEUE, METRICS_COMPACTOR_MAJC_STUCK, METRICS_SCAN_BUSY_TIMEOUT_COUNTER);
    // add sserver as flaky until scan server included in mini tests.
    Set<String> flakyMetrics = Set.of(METRICS_GC_WAL_ERRORS, METRICS_FATE_TYPE_IN_PROGRESS,
        METRICS_SCAN_BUSY_TIMEOUT_COUNTER, METRICS_SCAN_RESERVATION_TIMER,
        METRICS_SCAN_TABLET_METADATA_CACHE, METRICS_MANAGER_BALANCER_NEED_MIGRATION,
        METRICS_MANAGER_BALANCER_MIGRATING);

    Map<String,String> expectedMetricNames = this.getMetricFields();
    flakyMetrics.forEach(expectedMetricNames::remove); // might not see these
    unexpectedMetrics.forEach(expectedMetricNames::remove); // definitely shouldn't see these
    assertFalse(expectedMetricNames.isEmpty()); // make sure we didn't remove everything

    Map<String,String> seenMetricNames = new HashMap<>();

    List<String> statsDMetrics;

    // loop until we run out of lines or until we see all expected metrics
    while (!(statsDMetrics = sink.getLines()).isEmpty() && !expectedMetricNames.isEmpty()) {
      // for each metric name not yet seen, check if it is expected, flaky, or unknown
      statsDMetrics.stream().filter(line -> line.startsWith("accumulo"))
          .map(TestStatsDSink::parseStatsDMetric).map(Metric::getName)
          .filter(Predicate.not(seenMetricNames::containsKey)).forEach(name -> {
            if (expectedMetricNames.containsKey(name)) {
              // record expected metric names as seen, along with the value seen
              seenMetricNames.put(name, expectedMetricNames.remove(name));
            } else if (flakyMetrics.contains(name)) {
              // ignore any flaky metric names seen
              // these aren't always expected, but we shouldn't be surprised if we see them
            } else {
              // completely unexpected metric
              fail("Found accumulo metric not in expectedMetricNames or flakyMetricNames: " + name);
            }
          });
    }
    assertTrue(expectedMetricNames.isEmpty(),
        "Did not see all expected metric names, missing: " + expectedMetricNames.values());
  }

  private void doWorkToGenerateMetrics() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = this.getClass().getSimpleName();
      client.tableOperations().create(tableName);
      SortedSet<Text> splits = new TreeSet<>(List.of(new Text("5")));
      client.tableOperations().addSplits(tableName, splits);
      Thread.sleep(3_000);
      BatchWriterConfig config = new BatchWriterConfig().setMaxMemory(0);
      try (BatchWriter writer = client.createBatchWriter(tableName, config)) {
        Mutation m = new Mutation("row");
        m.put("cf", "cq", new Value("value"));
        writer.addMutation(m);
      }
      client.tableOperations().flush(tableName);
      try (BatchWriter writer = client.createBatchWriter(tableName, config)) {
        Mutation m = new Mutation("row");
        m.put("cf", "cq", new Value("value"));
        writer.addMutation(m);
      }
      client.tableOperations().flush(tableName);
      try (BatchWriter writer = client.createBatchWriter(tableName, config)) {
        for (int i = 0; i < 10; i++) {
          Mutation m = new Mutation(i + "_row");
          m.put("cf", "cq", new Value("value"));
          writer.addMutation(m);
        }
      }
      client.tableOperations().compact(tableName, new CompactionConfig());
      try (Scanner scanner = client.createScanner(tableName)) {
        scanner.forEach((k, v) -> {});
      }
      client.tableOperations().delete(tableName);
      while (client.tableOperations().exists(tableName)) {
        Thread.sleep(1000);
      }
    }
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    // unused; this class only extends MetricsProducer to easily reference its methods/constants
  }

  @Test
  public void metricTags() throws Exception {

    doWorkToGenerateMetrics();
    cluster.stop();

    List<String> statsDMetrics;

    // loop until we run out of lines or until we see all expected metrics
    while (!(statsDMetrics = sink.getLines()).isEmpty()) {
      statsDMetrics.stream().filter(line -> line.startsWith("accumulo"))
          .map(TestStatsDSink::parseStatsDMetric).forEach(a -> {
            var t = a.getTags();
            log.trace("METRICS, name: '{}' num tags: {}, tags: {}", a.getName(), t.size(), t);
            // check hostname is always set and is valid
            assertNotEquals("0.0.0.0", a.getTags().get("host"));
            assertNotNull(a.getTags().get("instance.name"));

            // check the length of the tag value is sane
            final int MAX_EXPECTED_TAG_LEN = 128;
            a.getTags().forEach((k, v) -> assertTrue(v.length() < MAX_EXPECTED_TAG_LEN));
          });
    }
  }
}
