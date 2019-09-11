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
package org.apache.accumulo.gc.metrics2;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.gc.thrift.GCStatus;

public class GcMetricsValues {

  private final Lock lock = new ReentrantLock();

  private GcCycleMetrics curr = new GcCycleMetrics();
  private GcCycleMetrics prev = new GcCycleMetrics();
  private GcCycleMetrics walCurr = new GcCycleMetrics();
  private GcCycleMetrics walPrev = new GcCycleMetrics();

  public GcMetricsValues() {}

  public void updateCollectStats() {
    lock.lock();
    try {
      prev = curr;
      curr = new GcCycleMetrics();
    } finally {
      lock.unlock();
    }
  }

  public void updateWalStats() {
    lock.lock();
    try {
      walPrev = walCurr;
      walCurr = new GcCycleMetrics();
    } finally {
      lock.unlock();
    }
  }

  public GcCycleMetrics getCurrent() {
    return curr;
  }

  public GcCycleMetrics getPrev() {
    return prev;
  }

  public GcCycleMetrics getWalCurr() {
    return walCurr;
  }

  public GcCycleMetrics getWalPrev() {
    return walPrev;
  }

  public GCStatus toThrift() {
    lock.lock();
    try {
      return new GCStatus(prev.toThrift(), walPrev.toThrift(), curr.toThrift(), walCurr.toThrift());
    } finally {
      lock.unlock();
    }
  }

}
