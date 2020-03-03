package org.apache.accumulo.server.conf.propstore.v3.metrics;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.BeforeClass;
import org.junit.Test;

public class PropStoreMetricsImplTest {

  private static PropStoreMetricsImpl metrics;

  @BeforeClass public static void init() {

    // the prefix is used to get the properties filename (hadoop-metrics2-accumulo.properties
    MetricsSystem metricsSystem = DefaultMetricsSystem
        .initialize("accumulo"); // called once per application

    metrics = new PropStoreMetricsImpl(metricsSystem);
    metrics.register();
  }

  @Test public void addLookupRate() {

    for (int i = 0; i < 10; i++) {

      StatTimer.record(metrics.lookupRate(), () -> {
        System.out.println("Hello"); return null;
      } );

      try (TimedStat ignored = metrics.timedLookup()) {

        metrics.addLookupRate(1000 + (100 * i));

        try {
          Thread.sleep(5_000);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      } // end timed section
    }
  }

}
