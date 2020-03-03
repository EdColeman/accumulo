package org.apache.accumulo.server.conf.propstore.v3.metrics;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropStoreMetricsImpl implements MetricsSource, PropStoreMetrics {

  private static final Logger log = LoggerFactory.getLogger(PropStoreMetricsImpl.class);

  private final MetricsSystem metricsSystem;
  private final MetricsRegistry registry;

  private static final String PROCESS = "A_PROCESS";
  private static final String NAME = "A_NAME";
  private static final String DESCRIPTION = "add some text";
  private static final String RECORD = "ZZ_record";
  private static final String CONTEXT = "master";

  private MutableRate lookupRate;
  private MutableCounterLong lookupHit;
  private MutableCounterLong lookupMiss;

  public PropStoreMetricsImpl(MetricsSystem metricsSystem) {

    this.metricsSystem = metricsSystem;

    this.registry = new MetricsRegistry(Interns.info(NAME, DESCRIPTION));
    this.registry.tag(MsInfo.ProcessName, PROCESS);

    lookupRate = registry.newRate("lookupRate", " some text for lookup rate");
    lookupHit = registry.newCounter("lookupHit", "found the lookup", 0L);
    lookupMiss = registry.newCounter("lookupMiss", "missed the lookup", 0L);
  }

  public void register() {

    log.info("Metrics system register() called");
    metricsSystem.register(NAME, DESCRIPTION, this);
  }

  @Override public void addLookupRate(long elapsed) {
    lookupRate.add(elapsed);
  }

  @Override public void incrLookupMiss() {
    lookupMiss.incr();
  }

  @Override public void incrLookupHit() {
    lookupHit.incr();
  }

  @Override public void zkRead(long elapsed) {

  }

  @Override public void zkWrite(long elapsed) {

  }

  @Override public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    log.info("Get metrics called");

    MetricsRecordBuilder builder = metricsCollector.addRecord(RECORD).setContext(CONTEXT);
    registry.snapshot(builder, all);
  }
}
