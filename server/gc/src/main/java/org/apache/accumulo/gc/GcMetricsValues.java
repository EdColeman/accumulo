/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.gc;

import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.gc.metrics2.GcCycleMetrics;

public class GcMetricsValues {

  private GCStatus status =
      new GCStatus(new GcCycleStats(), new GcCycleStats(), new GcCycleStats(), new GcCycleStats());

  private GcCycleMetrics curr = new GcCycleMetrics();
  private GcCycleMetrics prev = new GcCycleMetrics();
  private GcCycleMetrics walCurr = new GcCycleMetrics();
  private GcCycleMetrics walPrev = new GcCycleMetrics();

  public GcMetricsValues() {}

  public void updateCollectStats() {
    prev = curr;
    curr = new GcCycleMetrics();
  }

  public void updateWalStats() {
    walPrev = walCurr;
    walCurr = new GcCycleMetrics();
  }

  public GcCycleMetrics getCurrent() {
    return curr;
  }

  public GcCycleMetrics getWalCurr() {
    return walCurr;
  }
  // public void incrCurrentDeleted() {
  // ++status.current.deleted;
  // }
  //
  // public void incrCurrentErrors() {
  // ++status.current.errors;
  // }
  //
  // public void incrCurrentCandidates(long i) {
  // status.current.candidates += i;
  // }
  //
  // public void incrCurrentInUse(long i) {
  // status.current.inUse += i;
  // }
  //
  // public void setCurrentStarted() {
  // status.current.started = System.currentTimeMillis();
  // }
  //
  // public void setCurrentFinished() {
  // status.current.finished = System.currentTimeMillis();
  // }
  //
  // public void updateLast() {
  // status.last = status.current;
  // status.current = new GcCycleStats();
  // }
  //
  // public long getCurrentCandidates() {
  // return status.current.candidates;
  // }
  //
  // public long getCurrentInUse() {
  // return status.current.inUse;
  // }
  //
  // public long getCurrentDeleted() {
  // return status.current.deleted;
  // }
  //
  // public long getCurrentErrors() {
  // return status.current.errors;
  // }

  public GCStatus getStatus() {
    return new GCStatus(curr.toThrift(), prev.toThrift(), walCurr.toThrift(), walPrev.toThrift());
  }

}
