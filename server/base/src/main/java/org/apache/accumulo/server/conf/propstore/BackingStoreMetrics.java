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

import org.apache.accumulo.server.metrics.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.lib.MutableStat;

public class BackingStoreMetrics extends Metrics {

  // use common prefix, different that just gc, to prevent confusion with jvm gc metrics.
  public static final String METRIC_PREFIX = "ZkPropStore";

  private static final String jmxName = "ZkPropStore";
  private static final String description = "zookeeper property store metrics";
  private static final String record = "ZkPropStoreMetrics";

  private final BackingStore store;

  private final MutableRate loadCallRate;
  private final MutableStat zkLookupTimer;

  public BackingStoreMetrics(final BackingStore store) {
    super(jmxName + ",sub=" + store.getClass().getSimpleName(), description, "zkstore", record);

    this.store = store;
    MetricsRegistry registry = super.getRegistry();

    loadCallRate = registry.newRate("loadCallRate", "rate load calls to backing store");
    zkLookupTimer = registry.newStat("zkLookupTimer", "zookeeper lookup timing statistics",
        "zookeeper lookup", "nanoseconds");
  }

  public void loadCall() {
    loadCallRate.add(1L);
  }

  public void recordZkLookupTime(long nanos) {
    zkLookupTimer.add(nanos);
  }

}
