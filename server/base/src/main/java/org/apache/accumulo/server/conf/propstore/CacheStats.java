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
package org.apache.accumulo.server.conf.propstore;

import java.util.StringJoiner;

import org.apache.zookeeper.data.Stat;

class CacheStats {
  private Stat stat;
  private long lastUpdate;
  private long numUpdates;

  public CacheStats(final Stat stat) {
    this.stat = stat;
    lastUpdate = System.nanoTime();
    numUpdates = 0;
  }

  public Stat getExpectedVersion() {
    return stat;
  }

  public void setExpectedVersion(final Stat stat) {
    this.stat = stat;
    lastUpdate = System.nanoTime();
    numUpdates++;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public long getNumUpdates() {
    return numUpdates;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", CacheStats.class.getSimpleName() + "[", "]")
        .add("stat=" + ZooFunc.prettyStat(stat)).add("lastAccess=" + lastUpdate)
        .add("numUpdates=" + numUpdates).toString();
  }
}
