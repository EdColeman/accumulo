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
package org.apache.accumulo.test.functional;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.zookeeper.ZooReaderWriterFactory;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * IT Tests for development of an orphan file check that looks for files
 * not being removed by the Accumulo SimpleGarbageCollector.
 */
public class GcOrphanDevIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(GcOrphanDevIT.class);

  private static final int NUM_ROWS = 1000;
  private static final long SLOW_SCAN_SLEEP_MS = 0L;

  private Connector connector;

  private static final ExecutorService pool = Executors.newCachedThreadPool();

  private String tableName;

  private String secret;

  // Test development only. When true, multiple tables, multiple compactions will be
  // used during the test run which simulates transient condition that was causing
  // the test to fail..
  private boolean runMultipleCompactions = false;

  @Before
  public void setup() {

    connector = getConnector();

    tableName = getUniqueNames(1)[0];

    secret = cluster.getSiteConfiguration().get(Property.INSTANCE_SECRET);

    createData(tableName);
  }

  @AfterClass
  public static void cleanup() {
    pool.shutdownNow();
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  /**
   * Validate that {@code TableOperations} online operation does not block when table is already
   * online and fate transaction lock is held by other operations. The test creates, populates a
   * table and then runs a compaction with a slow iterator so that operation takes long enough to
   * simulate the condition. After the online operation while compaction is running completes, the
   * test is complete and the compaction is canceled so that other tests can run.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void checkScan() throws Exception {

    // launch a full table compaction with the slow iterator to ensure table lock is acquired and
    // held by the compaction

    Scanner scanner = connector.createScanner("accumulo.metadata", new Authorizations());
    // scanner.fetchColumnFamily(new Text("file"));

    for(Map.Entry<Key,Value> entry : scanner) {
      Key key = entry.getKey();
      Text row = entry.getKey().getRow();
      Value value = entry.getValue();

      log.debug("k:\'{}\', v:\'{}\'", key, value);
    }

    scanner.close();

    Future<?> compactTask = startCompactTask();

    // block if compaction still running
    compactTask.get();

    log.debug("---- after compaction ----");

    scanner = connector.createScanner("accumulo.metadata", new Authorizations());
    scanner.fetchColumnFamily(new Text("file"));

    Pattern p = Pattern.compile("(.+)/accumulo/tables/(.+)/(.*)/+(.+)");

    for(Map.Entry<Key,Value> entry : scanner) {
      Key key = entry.getKey();
      Text row = entry.getKey().getRow();
      Value value = entry.getValue();

      Matcher m = p.matcher(entry.getKey().getColumnQualifier().toString());
      if(m.matches()){
        log.debug("match");
        log.debug("root: {}", m.group(1));
        log.debug("table id: {}", m.group(2));
        log.debug("dir: {}", m.group(3));
        log.debug("file: {}", m.group(4));
      }
      log.debug("k:\'{}\', cq: \'{}\', v:\'{}\'", key.getRow(), entry.getKey().getColumnQualifier(), value);
    }

  }

  /**
   * This method was helpful for debugging a condition that was causing transient test failures.
   * This forces a condition that the test should be able to handle. This method is not needed
   * during normal testing, it was kept to aid future test development / troubleshooting if other
   * transient failures occur.
   */
  private void runMultipleCompactions() {

    for (int i = 0; i < 4; i++) {

      String aTableName = getUniqueNames(1)[0] + "_" + i;

      createData(aTableName);

      log.debug("Table: {}", aTableName);

      pool.submit(new SlowCompactionRunner(aTableName));

      assertTrue("verify that compaction running and fate transaction exists",
          blockUntilCompactionRunning(aTableName));

    }
  }

  /**
   * Create and run a slow running compaction task. The method will block until the compaction has
   * been started.
   *
   * @return a reference to the running compaction task.
   */
  private Future<?> startCompactTask() {
    Future<?> compactTask = pool.submit(new SlowCompactionRunner(tableName));
    //assertTrue("verify that compaction running and fate transaction exists",
    //    blockUntilCompactionRunning(tableName));
    return compactTask;
  }

  /**
   * Blocks current thread until compaction is running.
   *
   * @return true if compaction and associate fate found.
   */
  private boolean blockUntilCompactionRunning(final String tableName) {

    long maxWait = defaultTimeoutSeconds() <= 0 ? 60_000 : ((defaultTimeoutSeconds() * 1000) / 2);

    long startWait = System.currentTimeMillis();

    List<String> tservers = connector.instanceOperations().getTabletServers();

    /*
     * wait for compaction to start on table - The compaction will acquire a fate transaction lock
     * that used to block a subsequent online command while the fate transaction lock was held.
     */
    while (System.currentTimeMillis() < (startWait + maxWait)) {

      try {

        int runningCompactions = 0;

        for (String tserver : tservers) {
          runningCompactions += connector.instanceOperations().getActiveCompactions(tserver).size();
          log.trace("tserver {}, running compactions {}", tservers, runningCompactions);
        }

        if (runningCompactions > 0) {
          // Validate that there is a compaction fate transaction - otherwise test is invalid.
          if (findFate(tableName)) {
            return true;
          }
        }

      } catch (AccumuloSecurityException | AccumuloException ex) {
        throw new IllegalStateException("failed to get active compactions, test fails.", ex);
      } catch (KeeperException ex) {
        log.trace("Saw possible transient zookeeper error");
      }

      try {
        Thread.sleep(250);
      } catch (InterruptedException ex) {
        // reassert interrupt
        Thread.currentThread().interrupt();
      }
    }

    log.debug("Could not find compaction for {} after {} seconds", tableName,
        TimeUnit.MILLISECONDS.toSeconds(maxWait));

    return false;

  }

  /**
   * Checks fates in zookeeper looking for transaction associated with a compaction as a double
   * check that the test will be valid because the running compaction does have a fate transaction
   * lock.
   *
   * This method throws can throw either IllegalStateException (failed) or a Zookeeper exception.
   * Throwing the Zookeeper exception allows for retries if desired to handle transient zookeeper
   * issues.
   *
   * @param tableName
   *          a table name
   * @return true if corresponding fate transaction found, false otherwise
   * @throws KeeperException
   *           if a zookeeper error occurred - allows for retries.
   */
  private boolean findFate(final String tableName) throws KeeperException {

    Instance instance = connector.getInstance();
    AdminUtil<String> admin = new AdminUtil<>(false);

    try {

      String tableId = Tables.getTableId(instance, tableName);

      log.trace("tid: {}", tableId);

      IZooReaderWriter zk = new ZooReaderWriterFactory().getZooReaderWriter(
          instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut(), secret);
      ZooStore<String> zs = new ZooStore<>(ZooUtil.getRoot(instance) + Constants.ZFATE, zk);
      AdminUtil.FateStatus fateStatus = admin.getStatus(zs, zk,
          ZooUtil.getRoot(instance) + Constants.ZTABLE_LOCKS + "/" + tableId, null, null);

      log.trace("current fates: {}", fateStatus.getTransactions().size());

      for (AdminUtil.TransactionStatus tx : fateStatus.getTransactions()) {

        if (isCompaction(tx))
          return true;
      }

    } catch (TableNotFoundException | InterruptedException ex) {
      throw new IllegalStateException(ex);
    }

    // did not find appropriate fate transaction for compaction.
    return Boolean.FALSE;
  }

  /**
   * Test that the transaction top contains "CompactionDriver" and the debug message contains
   * "CompactRange"
   *
   * @param tx
   *          transaction status
   * @return true if tx top and debug have compaction messages.
   */
  private boolean isCompaction(AdminUtil.TransactionStatus tx) {

    if (tx == null) {
      log.trace("Fate tx is null");
      return false;
    }

    log.trace("Fate id: {}, status: {}", tx.getTxid(), tx.getStatus());

    String top = tx.getTop();
    String debug = tx.getDebug();

    return top != null && debug != null && top.contains("CompactionDriver")
        && tx.getDebug().contains("CompactRange");

  }

  /**
   * Returns the current table state (ONLINE, OFFLINE,...) of named table.
   *
   * @param tableName
   *          the table name
   * @return the current table state
   * @throws TableNotFoundException
   *           if table does not exist
   */
  private TableState getTableState(String tableName) throws TableNotFoundException {

    String tableId = Tables.getTableId(connector.getInstance(), tableName);

    TableState tstate = Tables.getTableState(connector.getInstance(), tableId);

    log.trace("tableName: '{}': tableId {}, current state: {}", tableName, tableId, tstate);

    return tstate;
  }

  /**
   * Create the provided table and populate with some data using a batch writer. The table is
   * scanned to ensure it was populated as expected.
   *
   * @param tableName
   *          the name of the table
   */
  private void createData(final String tableName) {

    try {

      // create table.
      connector.tableOperations().create(tableName);
      BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());

      // populate
      for (int i = 0; i < NUM_ROWS; i++) {
        Mutation m = new Mutation(new Text(String.format("%05d", i)));
        m.put(new Text("col" + ((i % 3) + 1)), new Text("qual"), new Value("junk".getBytes(UTF_8)));
        bw.addMutation(m);
      }
      bw.close();

      long startTimestamp = System.nanoTime();

      int count = scanCount(tableName);

      log.trace("Scan time for {} rows {} ms", NUM_ROWS, TimeUnit.MILLISECONDS
          .convert((System.nanoTime() - startTimestamp), TimeUnit.NANOSECONDS));

      if (count != NUM_ROWS) {
        throw new IllegalStateException(
            String.format("Number of rows %1$d does not match expected %2$d", count, NUM_ROWS));
      }
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException
        | TableExistsException ex) {
      throw new IllegalStateException("Create data failed with exception", ex);
    }
  }

  private int scanCount(String tableName) throws TableNotFoundException {

    Scanner scanner = connector.createScanner(tableName, Authorizations.EMPTY);
    int count = 0;
    for (Map.Entry<Key,Value> elt : scanner) {
      String expected = String.format("%05d", count);
      assert (elt.getKey().getRow().toString().equals(expected));
      count++;
    }

    scanner.close();

    return count;
  }

  /**
   * Instance to create / run a compaction using a slow iterator.
   */
  private class SlowCompactionRunner implements Runnable {

    private final String tableName;

    /**
     * Create an instance of this class.
     *
     * @param tableName
     *          the name of the table that will be compacted with the slow iterator.
     */
    SlowCompactionRunner(final String tableName) {
      this.tableName = tableName;
    }

    @Override
    public void run() {

      long startTimestamp = System.nanoTime();

      IteratorSetting slow = new IteratorSetting(30, "slow", SlowIterator.class);
      SlowIterator.setSleepTime(slow, SLOW_SCAN_SLEEP_MS);

      List<IteratorSetting> compactIterators = new ArrayList<>();
      compactIterators.add(slow);

      log.trace("Slow iterator {}", slow.toString());

      try {

        log.trace("Start compaction");

        connector.tableOperations().compact(tableName, new Text("0"), new Text("z"),
            compactIterators, true, true);

        log.trace("Compaction wait is complete");

        log.trace("Slow compaction of {} rows took {} ms", NUM_ROWS, TimeUnit.MILLISECONDS
            .convert((System.nanoTime() - startTimestamp), TimeUnit.NANOSECONDS));

        // validate that number of rows matches expected.

        startTimestamp = System.nanoTime();

        // validate expected data created and exists in table.

        int count = scanCount(tableName);

        log.trace("After compaction, scan time for {} rows {} ms", NUM_ROWS, TimeUnit.MILLISECONDS
            .convert((System.nanoTime() - startTimestamp), TimeUnit.NANOSECONDS));

        if (count != NUM_ROWS) {
          throw new IllegalStateException(
              String.format("After compaction, number of rows %1$d does not match expected %2$d",
                  count, NUM_ROWS));
        }

      } catch (TableNotFoundException ex) {
        throw new IllegalStateException("test failed, table " + tableName + " does not exist", ex);
      } catch (AccumuloSecurityException ex) {
        throw new IllegalStateException(
            "test failed, could not add iterator due to security exception", ex);
      } catch (AccumuloException ex) {
        // test cancels compaction on complete, so ignore it as an exception.
        if (!ex.getMessage().contains("Compaction canceled")) {
          throw new IllegalStateException("test failed with an Accumulo exception", ex);
        }
      }
    }
  }
}
