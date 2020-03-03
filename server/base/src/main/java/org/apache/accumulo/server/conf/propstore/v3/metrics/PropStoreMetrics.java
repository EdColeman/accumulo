package org.apache.accumulo.server.conf.propstore.v3.metrics;

public interface PropStoreMetrics {

  void addLookupRate(long elapsed);
  void incrLookupMiss();
  void incrLookupHit();

  void zkRead(long elapsed);
  void zkWrite(long elapsed);
}

