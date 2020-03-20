/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.metrics.micrometer;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Prototype showing different types of meters possible with micrometer using the simple, in-memory
 * registry.
 */
public class PublishTest {

  private static final Logger log = LoggerFactory.getLogger(PublishTest.class);

  private static MeterRegistry registry;

  @BeforeClass
  public static void init() {
    registry = new SimpleMeterRegistry();
  }

  @Test
  public void sample() throws Exception {

    AtomicInteger m1 = registry.gauge("sample.metric.m1", new AtomicInteger(0));
    Timer t2 = Timer.builder("sample.metric.t2").publishPercentiles(0.5, 0.95) // median and 95th
        // percentile
        .publishPercentileHistogram().register(registry);
    DistributionSummary summary = registry.summary("response.size");

    SampleRunner runner = new SampleRunner();

    for (int i = 0; i < 10; i++) {
      long nanos = System.nanoTime();
      Timer.Sample t1 = Timer.start(registry);
      m1.incrementAndGet();
      Thread.sleep(1_000);
      summary.record(i);
      t1.stop(registry.timer("sample.metric.t1"));
      long elapsed = System.nanoTime() - nanos;
      t2.record(Duration.ofNanos(elapsed));
    }

    runner.cleanup();
  }

  static class SampleRunner implements Runnable {

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public SampleRunner() {
      scheduler.scheduleAtFixedRate(this, 500, 750, TimeUnit.MILLISECONDS);
    }

    Consumer<Meter> out = a -> System.out.println(a.getId() + ": " + a.measure());

    @Override
    public void run() {
      registry.forEachMeter(out);
      try {
        Thread.sleep(750);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }

    public void cleanup() {
      if (scheduler != null) {
        scheduler.shutdownNow();
      }
    }
  }
}
