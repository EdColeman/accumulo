package org.apache.accumulo.server.metrics.registry;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.step.StepMeterRegistry;

import java.util.concurrent.TimeUnit;

/**
 * This class provides sending micrometer metrics to a file - mainly for development and debugging.
 * For production it is expected that one of the provided registries (prometheus, statsd,...)
 * <p>
 * This class has been modeled after the io.micrometer.core.instrument.logging.LoggingMeterRegistry
 * that uses log4j to publish the metrics.
 */
public class MetricsFileRegistry extends StepMeterRegistry {

  public MetricsFileRegistry() {
    this(MetricsFileRegistryConfig.DEFAULT, Clock.SYSTEM);
  }

  public MetricsFileRegistry(MetricsFileRegistryConfig config, Clock clock) {
    super(config, clock);
  }

  @Override protected void publish() {

  }

  @Override protected TimeUnit getBaseTimeUnit() {
    return TimeUnit.MILLISECONDS;
  }
}
