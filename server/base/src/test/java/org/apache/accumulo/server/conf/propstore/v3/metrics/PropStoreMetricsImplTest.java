package org.apache.accumulo.server.conf.propstore.v3.metrics;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.BeforeClass;
import org.junit.Test;

public class PropStoreMetricsImplTest {

  private static PropStoreMetricsImpl metrics;

  @BeforeClass public static void init() {

    MetricsSystem metricsSystem = DefaultMetricsSystem
        .initialize("test"); // called once per application

    metrics = new PropStoreMetricsImpl(metricsSystem);
    metrics.register();
  }

  @Test public void addLookupRate() {

    metrics.addLookupRate(1000);

    try {
      Thread.sleep(20_000);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }
}
