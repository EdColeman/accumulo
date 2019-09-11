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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.gc.thrift.GcCycleStats;

public class GcCycleMetrics {

  private AtomicLong started = new AtomicLong(0);
  private AtomicLong finished = new AtomicLong(0);
  private AtomicLong candidates = new AtomicLong(0);
  private AtomicLong inUse = new AtomicLong(0);
  private AtomicLong deleted = new AtomicLong(0);
  private AtomicLong errors = new AtomicLong(0);

  // not in thrift
  private ReentrantLock errorLock = new ReentrantLock();
  private boolean sawCollectionError = Boolean.FALSE;
  private String errMsg = "";

  public GcCycleMetrics() {}

  public long getStarted() {
    return started.get();
  }

  public void markStarted() {
    started.set(System.currentTimeMillis());
  }

  public long getFinished() {
    return finished.get();
  }

  public void markFinished() {
    finished.set(System.currentTimeMillis());
  }

  public long getCandidates() {
    return candidates.get();
  }

  public void incrementCandidates() {
    candidates.incrementAndGet();
  }

  public void incrementCandidates(final long delta) {
    candidates.addAndGet(delta);
  }

  public long getDeleted() {
    return deleted.get();
  }

  public long getInUse() {
    return inUse.get();
  }

  public void incrementInUse() {
    inUse.incrementAndGet();
  }

  public void incrementInUse(final long delta) {
    inUse.addAndGet(delta);
  }

  public long getDeletes() {
    return deleted.get();
  }

  public void incrementDeleted() {
    deleted.incrementAndGet();
  }

  public void incrementDeleted(final long delta) {
    deleted.addAndGet(delta);
  }

  public long getErrors() {
    return errors.get();
  }

  public void incrementErrors() {
    errors.incrementAndGet();
  }

  public void incrementErrors(final long delta) {
    errors.addAndGet(delta);
  }

  public void sawCollectionError(final String msg) {
    errorLock.lock();
    try {
      sawCollectionError = Boolean.TRUE;
      errMsg = msg;
      markFinished();
    } finally {
      errorLock.unlock();
    }
  }

  public boolean hasError() {
    errorLock.lock();
    try {
      return sawCollectionError;
    } finally {
      errorLock.unlock();
    }
  }

  public String getErrMsg() {
    errorLock.lock();
    try {
      return errMsg;
    } finally {
      errorLock.unlock();
    }
  }

  public GcCycleStats toThrift() {
    GcCycleStats stat = new GcCycleStats();
    stat.started = started.get();
    stat.finished = finished.get();
    stat.candidates = candidates.get();
    stat.inUse = inUse.get();
    stat.deleted = deleted.get();
    stat.errors = errors.get();
    return stat;
  }

}
