/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IteratorExceptionIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(FateConcurrencyIT.class);

  private static final int NUM_DATA_ROWS = 10;

  private AccumuloClient client;
  private ClientContext context;

  private static final ExecutorService pool = Executors.newCachedThreadPool();
  private String secret;

  private long maxWaitMillis;

  @BeforeEach
  public void setup() {
    client = Accumulo.newClient().from(getClientProps()).build();
    context = (ClientContext) client;
    secret = cluster.getSiteConfiguration().get(Property.INSTANCE_SECRET);
    maxWaitMillis = Math.max(MINUTES.toMillis(1), defaultTimeout().toMillis() / 2);
  }

  @AfterEach
  public void closeClient() {
    client.close();
  }

  @AfterAll
  public static void cleanup() {
    pool.shutdownNow();
  }

  @Test
  public void cleanScan() throws Exception {
    String tableName = getUniqueNames(1)[0];
    createData(tableName);
    client.tableOperations().flush(tableName, null, null, false);
    sleepUninterruptibly(1, TimeUnit.SECONDS);

    IteratorSetting is = new IteratorSetting(30, ErrorThrowingIterator.class);
    client.tableOperations().attachIterator(tableName, is,
        EnumSet.of(IteratorUtil.IteratorScope.scan, IteratorUtil.IteratorScope.minc,
            IteratorUtil.IteratorScope.majc));

    assertEquals(NUM_DATA_ROWS, scanCount(tableName));
  }

  private void createData(final String tableName) throws AccumuloException, TableExistsException,
      AccumuloSecurityException, TableNotFoundException {
    client.tableOperations().create(tableName);
    log.info("Created table id: {}, name \'{}\'",
        client.tableOperations().tableIdMap().get(tableName), tableName);
    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      // populate
      for (int i = 0; i < NUM_DATA_ROWS; i++) {
        Mutation m = new Mutation(new Text(String.format("%05d", i)));
        m.put("col" + ((i % 3) + 1), "qual", i + ":junk");
        bw.addMutation(m);
      }
    }
  }

  private int scanCount(final String tableName) {
    try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
      int count = 0;
      for (Map.Entry<Key,Value> elt : scanner) {
        String expected = String.format("%05d", count);
        assert elt.getKey().getRow().toString().equals(expected);
        count++;
      }
      return count;
    } catch (TableNotFoundException ex) {
      log.debug("cannot verify row count, table \'{}\' does not exist", tableName);
      throw new IllegalStateException(ex);
    }
  }
}
