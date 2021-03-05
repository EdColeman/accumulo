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

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.server.conf2.CacheId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropReader extends PropWorker {
  private static final Logger log = LoggerFactory.getLogger(PropReader.class);
  private final long seed = System.nanoTime();

  private int counter;
  private int runningHash;

  public PropReader(final String zkConnString, final String workerId, final String instanceId,
      final ExpectedValues truth) throws Exception {
    super(zkConnString, workerId, instanceId, truth);
  }

  @Override
  public void doWork() {
    log.debug("tock: {}", super.getWorkerId());

    try {

      ExpectedValues truth = getExpectedValues();

      log.debug("truth {}", truth);
      CacheId id = pickRandomTable();

      log.debug("Random id: {}", id);

      synchronized (this) {

        try {

          ExpectedProps expected = truth.readExpected(id);
          log.debug("Have expected: {}", expected);

          var actual = readPropsFromStore(id);

          var intValue = actual.get(ExpectedValues.PROP_INT_NAME);

          log.debug("Actual: {}, Value: {} - setting {}", actual, intValue,
              expected.getExpectedInt());

          if ("-1".equals(intValue)) {
            Map<String,String> updates = new HashMap<>(actual);
            updates.put(ExpectedValues.PROP_INT_NAME, Integer.toString(expected.getExpectedInt()));
            writePropsToStore(id, updates);
            return;
          }

          log.debug("expected: {}, actual {}", expected, actual);

          while (expected.isUpdating()) {
            wait(500);
            expected = truth.readExpected(id);
          }

          wait(WORKER_DELAY);

        } catch (InterruptedException ex) {
          // running.set(false);
          throw new IllegalStateException("quiting worker thread");
        }
      }
    } catch (Exception ex) {
      log.info("What: ", ex);
    }
  }
}
