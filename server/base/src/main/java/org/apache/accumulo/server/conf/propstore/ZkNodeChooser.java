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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.data.TableId;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class ZkNodeChooser {

  // TODO - replace with Constants and allow for instance id.
  private static final String ZK_TABLE_CONFIG_ROOT = "/accumulo/1234/table_config";
  private static final String NODE_NAME_PREFIX = "Sentinel";

  private static int expectedTables = 10_000;
  private static int targetChildNodesMax = 50;

  private static int mask = mask(expectedTables, targetChildNodesMax);

  private static HashFunction hf = Hashing.murmur3_128(0);

  public static String getWatcherPath(TableId tableId) {
    HashCode hc = hf.newHasher().putString(tableId.canonical(), UTF_8).hash();
    int hv = hc.asInt();
    // int x = (hv >> 16) ^ hv;
    // x = tableId.canonical().hashCode();
    return String.format("%s/%s_%04d", ZK_TABLE_CONFIG_ROOT, NODE_NAME_PREFIX, hv & mask);
  }

  public static int getNumBins() {
    return mask + 1;
  }

  /**
   * Calculates a mask for bitwise AND that uses an expected number of zookeeper nodes and a target
   * fanout. The mask is the nearest power of 2 - 1 (equivalent to 2**(bits for expected / fanout) -
   * 1. Examples:
   *
   * <pre>
   *   7 nodes, fanout of 1 would result in a mask of 0x07.
   *   8 nodes, fanout of 1 would result in a mask of 0x0f.
   *   1,000 nodes and a fanout of 25 would have a target of 40 nodes, the mask is 0x3f (63 base 10).
   * </pre>
   * <p>
   * The mask is expected to be used with a hash to determine a bucket, so the number of child
   * target nodes is more of an estimate and a lot will depend on the quality of the hash algorithm.
   *
   * @param expected
   *          the expected number of tables
   * @param fanout
   *          a target fanout for number of child nodes
   * @return the mask value.
   */
  static int mask(final int expected, final int fanout) {
    int x = expected / fanout;
    int shift = 31 - Integer.numberOfLeadingZeros(x);
    return (1 << (shift + 1)) - 1;
  }
}
