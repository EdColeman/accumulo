package org.apache.accumulo.server.conf.propstore.v3.metrics;

import org.apache.hadoop.metrics2.lib.MutableStat;

import java.util.function.Supplier;

public interface StatTimer {

  static <T> T record(MutableStat stat, Supplier<T> f){
    long start = System.nanoTime();
    T t = f.get();
    stat.add(System.nanoTime() - start);
    return f.get();
  }
}
