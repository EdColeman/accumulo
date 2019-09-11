package org.apache.accumulo.gc.metrics2;

import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GcMetricsValuesTest {

  @Test public void empty() {
    GcMetricsValues gmv = new GcMetricsValues();

    assertEquals(0L, gmv.getCurrent().getStarted());
    assertEquals(0L, gmv.getCurrent().getCandidates());

    assertEquals(0L, gmv.getPrev().getStarted());
    assertEquals(0L, gmv.getWalCurr().getStarted());
    assertEquals(0L, gmv.getWalPrev().getStarted());

  }

  @Test public void updateCollectStats() {

    GcMetricsValues gmv = new GcMetricsValues();
    gmv.getCurrent().markStarted();
    gmv.getCurrent().incrementInUse();

    // before update
    assertTrue((gmv.getCurrent().getStarted() > 0) && (gmv.getCurrent().getStarted() <= System
        .currentTimeMillis()));
    assertEquals(1L, gmv.getCurrent().getInUse());

    assertEquals(0L, gmv.getPrev().getStarted());
    assertEquals(0L, gmv.getWalCurr().getStarted());
    assertEquals(0L, gmv.getWalPrev().getStarted());

    gmv.updateCollectStats();

    // after
    assertTrue((gmv.getPrev().getStarted() > 0) && (gmv.getCurrent().getStarted() <= System
        .currentTimeMillis()));

    assertEquals(1L, gmv.getPrev().getInUse());

    assertEquals(0L, gmv.getCurrent().getStarted());
    assertEquals(0L, gmv.getWalCurr().getStarted());
    assertEquals(0L, gmv.getWalPrev().getStarted());

  }

  @Test public void updateWalStats() {

    GcMetricsValues gmv = new GcMetricsValues();
    gmv.getWalCurr().markStarted();
    gmv.getWalCurr().incrementInUse();

    // before update
    assertTrue((gmv.getWalCurr().getStarted() > 0) && (gmv.getCurrent().getStarted() <= System
        .currentTimeMillis()));
    assertEquals(1L, gmv.getWalCurr().getInUse());

    assertEquals(0L, gmv.getCurrent().getStarted());
    assertEquals(0L, gmv.getPrev().getStarted());
    assertEquals(0L, gmv.getWalPrev().getStarted());

    gmv.updateWalStats();

    // after
    assertTrue((gmv.getWalPrev().getStarted() > 0) && (gmv.getWalPrev().getStarted() <= System
        .currentTimeMillis()));

    assertEquals(1L, gmv.getWalPrev().getInUse());

    assertEquals(0L, gmv.getCurrent().getStarted());
    assertEquals(0L, gmv.getPrev().getStarted());
    assertEquals(0L, gmv.getWalCurr().getStarted());
  }

  @Test public void getCurrent() {
  }

  @Test public void getWalCurr() {
  }

  @Test public void getStatus() {

    GcMetricsValues gmv = new GcMetricsValues();

    gmv.getCurrent().markStarted();
    gmv.getCurrent().incrementCandidates();
    gmv.getCurrent().markFinished();

    gmv.getWalCurr().markStarted();
    gmv.getWalCurr().incrementInUse();
    gmv.getWalCurr().markFinished();

    gmv.updateCollectStats();
    gmv.updateWalStats();

    gmv.getCurrent().markStarted();
    gmv.getCurrent().incrementErrors();

    gmv.getWalCurr().markStarted();
    gmv.getWalCurr().incrementDeleted();

    GCStatus thrift = gmv.toThrift();

    assertTrue(thrift.current.started > 0 && thrift.current.started <= System.currentTimeMillis());
    assertEquals(0L, thrift.current.finished);
    assertEquals(1, thrift.current.errors);

    assertTrue(thrift.last.started > 0 && thrift.last.started <= System.currentTimeMillis());
    assertTrue(thrift.last.finished > 0 && thrift.last.finished <= System.currentTimeMillis());
    assertEquals(1, thrift.last.candidates);


    assertTrue(thrift.currentLog.started > 0 && thrift.currentLog.started <= System.currentTimeMillis());
    assertEquals(0L, thrift.currentLog.finished);
    assertEquals(1, thrift.currentLog.deleted);

    assertTrue(thrift.lastLog.started > 0 && thrift.lastLog.started <= System.currentTimeMillis());
    assertTrue(thrift.lastLog.finished > 0 && thrift.lastLog.finished <= System.currentTimeMillis());
    assertEquals(1, thrift.lastLog.inUse);

  }
}
