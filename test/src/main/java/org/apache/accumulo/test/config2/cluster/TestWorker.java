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
package org.apache.accumulo.test.config2.cluster;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class TestWorker implements Runnable {

  private final String zkConnString;
  private final String workerId;

  private final AtomicBoolean running = new AtomicBoolean(true);

  public TestWorker(final String zkConnString, final String workerId) {
    this.zkConnString = zkConnString;
    this.workerId = workerId;
  }

  public String getWorkerId() {
    return workerId;
  }

  public boolean isRunning() {
    return running.get();
  }

  @Override
  public void run() {
    while (running.get()) {
      doWork();
    }
  }

  public abstract void doWork();

  public void quit() {
    running.set(false);
  }
}
