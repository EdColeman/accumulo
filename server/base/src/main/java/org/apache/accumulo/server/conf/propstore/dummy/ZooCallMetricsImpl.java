/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.conf.propstore.dummy;

import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicLong;

public class ZooCallMetricsImpl implements ZooCallMetrics {

  private AtomicLong createCount = new AtomicLong();
  private AtomicLong existsCount = new AtomicLong();
  private AtomicLong getDataCount = new AtomicLong();
  private AtomicLong setDataCount = new AtomicLong();
  private AtomicLong errorCount = new AtomicLong();

  public ZooCallMetricsImpl() {}

  @Override
  public void incrCreateCount() {
    createCount.incrementAndGet();
  }

  @Override
  public void incrErrorCount() {
    errorCount.incrementAndGet();
  }

  @Override
  public void incrExistsCount() {
    existsCount.incrementAndGet();
  }

  @Override
  public void incrGetDataCount() {
    createCount.incrementAndGet();
  }

  @Override
  public void incrSetDataCount() {
    setDataCount.incrementAndGet();
  }

  @Override
  public long getCreateCount() {
    return createCount.get();
  }

  @Override
  public long getErrorCount() {
    return errorCount.get();
  }

  @Override
  public long getExistsCount() {
    return existsCount.get();
  }

  @Override
  public long getGetDataCount() {
    return getDataCount.get();
  }

  @Override
  public long getSetDataCount() {
    return setDataCount.get();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ZooCallMetricsImpl.class.getSimpleName() + "[", "]")
        .add("createCount=" + getCreateCount()).add("existsCount=" + getExistsCount())
        .add("getDataCount=" + getGetDataCount()).add("setDataCount=" + getSetDataCount())
        .add("errorCount=" + getErrorCount()).toString();
  }
}
