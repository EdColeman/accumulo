package org.apache.accumulo.server.metrics.registry;

import io.micrometer.core.instrument.step.StepRegistryConfig;

public interface MetricsFileRegistryConfig extends StepRegistryConfig {

  MetricsFileRegistryConfig DEFAULT = k -> null;

  @Override
  default String prefix() {
    return "file";
  }

  /**
   * @return Whether counters and timers that have no activity in an
   * interval are still logged.
   */
  default boolean logInactive() {
    String v = get(prefix() + ".logInactive");
    return Boolean.parseBoolean(v);
  }

  default String filePath(){
    String v = get(prefix() + ".filePath");
    if(v == null || v.isEmpty()){
      return "/tmp/all.metrics.out";
    }
    return v;
  }

}
