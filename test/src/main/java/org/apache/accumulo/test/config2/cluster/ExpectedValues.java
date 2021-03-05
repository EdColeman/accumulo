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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.PropCacheException;
import org.apache.accumulo.server.conf2.impl.ZooPropStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Provides a source of "truth" of property values for multi-thread testing. The values maintained
 * by an instance of the class should be read / written to an external store.
 */
public class ExpectedValues {

  private static final Logger log = LoggerFactory.getLogger(ExpectedValues.class);

  public static final String PROP_INT_NAME =
      Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "intValue";
  public static final String PROP_UPDATE_NAME =
      Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "lastUpdate";

  private final Map<CacheId,Guard> nodes;

  private static final AtomicInteger totalReads = new AtomicInteger(0);
  private static final AtomicInteger totalWrites = new AtomicInteger(0);
  private static final AtomicInteger totalErrors = new AtomicInteger(0);

  public ExpectedValues(final ServerContext context, final int numTables) {

    if (numTables < 1) {
      throw new IllegalArgumentException("Number of tables must be > 1, received: " + numTables);
    }

    nodes = new HashMap<>();
    IntStream.range(0, numTables)
        .mapToObj(i -> CacheId.forTable(context, TableId.of(Integer.toString(i))))
        .forEach(id -> nodes.put(id, new Guard()));
  }

  public Collection<CacheId> getIds() {
    return Collections.unmodifiableCollection(nodes.keySet());
  }

  @SuppressFBWarnings(value = "PREDICTABLE_RANDOM",
      justification = "testing - not a secure context")
  public CacheId pickRandomTable() {
    var ids = new ArrayList<>(nodes.keySet());

    Random rand = ThreadLocalRandom.current();

    return ids.get(rand.nextInt(ids.size()));
  }

  public int numLocks() {
    return nodes.size();
  }

  public static int getTotalReads() {
    return totalReads.get();
  }

  public static int getTotalWrites() {
    return totalWrites.get();
  }

  public static int getTotalErrors() {
    return totalErrors.get();
  }

  /**
   * Read the internal cache to get the value that should be also stored in external store.
   *
   * @param id
   *          a CacheId
   * @return the expected property values
   */
  public ExpectedProps readExpected(final CacheId id) {
    Objects.requireNonNull("A CacheId must be provided");
    var expected = nodes.get(id);
    Objects.requireNonNull(expected, "CacheId not present in map. Provided: " + id.toString());

    return expected.readExpected();
  }

  /**
   * Start an internal transaction to update values. An open transaction is marked as isUpdating.
   * When the external store is updated, the transaction should be closed by calling
   * endTransaction().
   *
   * @param id
   *          a CacheId
   * @return the values that should be written to the external store.
   */
  private ExpectedProps startUpdate(final CacheId id) {
    Objects.requireNonNull("A CacheId must be provided");
    var expected = nodes.get(id);
    return expected.startUpdate();
  }

  private void finishUpdate(final CacheId id) {
    Objects.requireNonNull("A CacheId must be provided");
    var expected = nodes.get(id);
    expected.finishUpdate();
  }

  public Map<String,String> updateStore(final ZooPropStore store, final CacheId id) {

    try {

      var expectedProp = nodes.get(id);

      startUpdate(id);

      Map<String,String> storedProps = store.readFromStore(id).getAllProperties();

      validateProps(expectedProp, storedProps);

      Map<String,String> updatedProps = updateProps(storedProps, expectedProp);

      store.setProperties(id, updatedProps);

      log.debug("Now: {}", expectedProp);
    } catch (PropCacheException ex) {
      throw new IllegalStateException("Prop update failed", ex);
    } finally {
      finishUpdate(id);
    }

    return null;
  }

  private void validateProps(Guard expectedProp, Map<String,String> storedProps) {
    long now = System.nanoTime();

    boolean valid = true;

    int expInt = expectedProp.props.getExpectedInt();

    log.debug("Looking for {} in {}", ExpectedValues.PROP_INT_NAME, storedProps);

    if (!storedProps.containsKey(ExpectedValues.PROP_INT_NAME)) {
      log.debug("Stored props may not have been saved (expected on first store) - {}", storedProps);
      // no stored data
      return;
    }

    var asString = storedProps.get(ExpectedValues.PROP_INT_NAME);

    log.warn("A STRING: {} would like to match {}", asString, expInt);

    int sv = Integer.parseInt(asString);

    log.warn("An INT: {} = {}", sv, expInt);

    if (expInt != sv + 1) {
      log.warn("Expected int: {} did not match stored: {}", expInt, sv);
      valid = false;
    }

    long en = now - expectedProp.props.getLastUpdate();
    long sn = now - Long.parseLong(storedProps.get(ExpectedValues.PROP_UPDATE_NAME));

    if (en > sn) {
      log.warn(
          "Expected update time delta {} is greater than the stored update time {} - possible out of order update?",
          en, sn);
      valid = false;
    }

    if (!valid) {
      totalErrors.incrementAndGet();
    }
  }

  public Map<String,String> updateProps(Map<String,String> current, Guard guard) {

    Map<String,String> updates = new HashMap<>(current);

    ExpectedProps props = guard.props;

    updates.put(ExpectedValues.PROP_INT_NAME, Integer.toString(props.getExpectedInt()));
    updates.put(ExpectedValues.PROP_UPDATE_NAME, Long.toString(props.getLastUpdate()));

    return updates;
  }

  private static class Guard {

    private final ReadWriteLock rwLock;
    private final Lock rLock;
    private final Lock wLock;

    // access guarded by rw lock.
    private ExpectedProps props;
    private boolean isUpdating;

    public Guard() {
      rwLock = new ReentrantReadWriteLock();
      rLock = rwLock.readLock();
      wLock = rwLock.writeLock();

      props = new ExpectedProps(System.nanoTime(), 0, false);
    }

    public ExpectedProps readExpected() {
      try {
        rLock.lock();
        totalReads.incrementAndGet();
        return props;
      } finally {
        rLock.unlock();
      }
    }

    public ExpectedProps startUpdate() {
      try {

        wLock.lock();
        totalWrites.incrementAndGet();

        if (isUpdating) {
          totalErrors.incrementAndGet();
          throw new IllegalStateException(
              "Invalid concurrent access, update currently in progress");
        }

        isUpdating = true;

        ExpectedProps next = new ExpectedProps(System.nanoTime(), props.getExpectedInt() + 1, true);

        props = next;

        return next;

      } finally {
        wLock.unlock();
      }
    }

    public void finishUpdate() {
      try {
        totalWrites.incrementAndGet();
        wLock.lock();
        if (!isUpdating) {
          totalErrors.incrementAndGet();
          throw new IllegalStateException(
              "Invalid concurrent access, tried to close update that was not in progress");
        }
        isUpdating = false;
      } finally {
        wLock.unlock();
      }
    }
  }
}
