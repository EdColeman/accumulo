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

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ZkAgitator extends TestWorker {

  private static final Logger log = LoggerFactory.getLogger(ZkAgitator.class);

  private final ArrayList<InstanceSpec> zkInstances;

  private final int tickTime = 5_000;

  private final TestingCluster zkTestCluster;

  public ZkAgitator(final TestingCluster cluster, final String workerId) {
    super(cluster.getConnectString(), workerId);
    this.zkTestCluster = cluster;

    zkInstances = new ArrayList<>(cluster.getInstances());
  }

  @SuppressFBWarnings(value = "PREDICTABLE_RANDOM",
      justification = "testing - not a secure context")
  @Override
  public void doWork() {

    while (isRunning()) {

      // test is not a secure context - improve test coverage

      Random rand = ThreadLocalRandom.current();

      // pick a server;
      var index = rand.nextInt(zkInstances.size());
      var serverSpec = zkInstances.get(index);

      log.info("Stopping: {} - {}", index, serverSpec);

      var delay = getDelay();
      synchronized (this) {
        try {
          var killed = zkTestCluster.killServer(serverSpec);
          log.info("Killed: {}", killed);
          log.debug("Sleeping for {}", delay);
          wait(delay);
          var found = zkTestCluster.restartServer(serverSpec);
          log.info("restart - server found: {}", found);
          // pause to allow recovery
          wait((long) tickTime * 30);
          zkTestCluster.getServers().forEach(s -> log.debug("quorum peer: {}", s.getQuorumPeer()));

        } catch (InterruptedException ex) {
          throw new IllegalStateException("ZkAgitator - exit because of interrupt", ex);
        } catch (Exception ex) {
          throw new IllegalStateException("ZkAgitator - exit because of exception", ex);
        }
      }
    }
  }

  /**
   * Random delay between 1/2 and 3x the tick time
   */
  @SuppressFBWarnings(value = "PREDICTABLE_RANDOM",
      justification = "testing - not a secure context")
  private int getDelay() {
    var minDelay = tickTime / 2;
    var maxDelay = tickTime * 3;
    Random rand = ThreadLocalRandom.current();
    return rand.nextInt(maxDelay + minDelay) + minDelay;
  }
}
