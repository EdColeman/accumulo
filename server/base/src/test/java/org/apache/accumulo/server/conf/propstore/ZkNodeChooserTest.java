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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.data.TableId;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkNodeChooserTest {

  private static final Logger log = LoggerFactory.getLogger(ZkNodeChooserTest.class);

  @Test
  public void mask7() {
    assertEquals(7, ZkNodeChooser.mask(7, 1));
  }

  @Test
  public void mask8() {
    assertEquals(7, ZkNodeChooser.mask(7, 1));
  }

  @Test
  public void mask255() {
    assertEquals(255, ZkNodeChooser.mask(255, 1));
  }

  @Test
  public void mask1023() {
    int mask = ZkNodeChooser.mask(8191, 8);
    assertEquals(0x03ff, mask);
  }

  @Test
  public void mask1024() {
    int mask = ZkNodeChooser.mask(8192, 8);
    assertEquals(0x07ff, mask);
  }

  @Test
  public void smallDist() {
    int numNodes = 100;
    Map<String,Integer> m = fillBins(numNodes);

    printStats(m);

  }

  @Test
  public void medDist() {
    int numNodes = 4096;
    Map<String,Integer> m = fillBins(numNodes);

    printStats(m);

    Set<String> s = new TreeSet<>(m.keySet());
    for (String n : s) {
      log.info("{}", n);
    }
  }

  private void printStats(final Map<String,Integer> m) {

    int numBins = m.size();

    DescriptiveStatistics sd = new DescriptiveStatistics();
    for (Map.Entry<String,Integer> e : m.entrySet()) {
      sd.addValue(e.getValue());
      log.info("{} : {}", e.getKey(), e.getValue());
    }

    log.info("number of bins, target: {}, actual: {}", ZkNodeChooser.getNumBins(), numBins);
    // log.info("est of bin count: {}", numNodes / ZkNodeChooser.getNumBins());

    log.info("mean {}", sd.getMean());
    log.info("K {}", sd.getKurtosis());
    log.info("G {}", sd.getGeometricMean());
    log.info("V {}", sd.getPopulationVariance());

  }

  private Map<String,Integer> fillBins(final int numNodes) {
    Map<String,Integer> m = new HashMap<>();

    for (int i = 0; i < numNodes; i++) {
      String v = base62Encode(i);
      String n = ZkNodeChooser.getWatcherPath(TableId.of(v));
      m.merge(n, 1, Integer::sum);
    }

    return m;
  }

  @Test
  public void distribution() {
    int numNodes = 10_000_000;
    Map<String,Integer> m = fillBins(numNodes);
    printStats(m);
  }

  /**
   * Generate plausible Accumulo table ids.
   */
  public static final String ALPHABET =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  public static final int BASE = ALPHABET.length();

  private String base62Encode(long value) {
    StringBuilder sb = new StringBuilder();
    while (value != 0) {
      sb.append(ALPHABET.charAt((int) (value % 62)));
      value /= 62;
    }
    while (sb.length() < 6) {
      sb.append(0);
    }
    return sb.reverse().toString();
  }
}
