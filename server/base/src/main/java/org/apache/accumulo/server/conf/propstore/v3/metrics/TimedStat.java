package org.apache.accumulo.server.conf.propstore.v3.metrics;

import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.lib.MutableStat;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

public class TimedStat implements AutoCloseable {

  private static Consumer fn;
  private final MutableStat stat;

  private final long start;

  public TimedStat(final MutableStat stat){
    this.stat = stat;
    start = System.nanoTime();
  }

  @Override public void close() {
    stat.add(TimeUnit.MILLISECONDS.convert((System.nanoTime() - start),TimeUnit.NANOSECONDS));
  }
}
