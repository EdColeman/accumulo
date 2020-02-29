package org.apache.accumulo.server.conf.propstore.v3.metrics;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropStoreMetricsImpl implements MetricsSource, PropStoreMetrics {

  private static final Logger log = LoggerFactory.getLogger(PropStoreMetricsImpl.class);

  private final MetricsSystem metricsSystem;
  private final MetricsRegistry registry;

  private final static String NAME = "A_NAME";
  private final static String DESCRIPTION = "add some text";
  public static final String RECORD = "record";
  public static final String CONTEXT = "context";

  private MutableRate lookupRate;

  public PropStoreMetricsImpl(MetricsSystem metricsSystem) {
    this.metricsSystem = metricsSystem;

    this.registry = new MetricsRegistry(Interns.info(NAME, DESCRIPTION));
    this.registry.tag(MsInfo.ProcessName,"A_PROCESS");

    lookupRate = registry.newRate("lookup_rate", " some text for lookup rate");
  }

  public void register() {
    metricsSystem.register(NAME, DESCRIPTION, this);
  }

  @Override public void addLookupRate(long elapsed) {
    lookupRate.add(elapsed);
  }

  @Override public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    log.info("Get metrics called");

    MetricsRecordBuilder builder = metricsCollector.addRecord(RECORD).setContext(CONTEXT);
    registry.snapshot(builder, all);
  }
}
