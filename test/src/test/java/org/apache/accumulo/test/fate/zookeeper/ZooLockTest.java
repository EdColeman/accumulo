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
package org.apache.accumulo.test.fate.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Field;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooLock.AsyncLockWatcher;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import com.google.common.base.Stopwatch;
/**
 *
 */
public class ZooLockTest {

  private static final TemporaryFolder folder =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  private static MiniAccumuloCluster accumulo;

  static class ConnectedWatcher implements Watcher {
    volatile boolean connected = false;

    @Override
    public synchronized void process(WatchedEvent event) {
      if (event.getState() == KeeperState.SyncConnected) { // For ZK >3.4.... || event.getState() ==
                                                           // KeeperState.ConnectedReadOnly) {
        connected = true;
      } else {
        connected = false;
      }
    }

    public synchronized boolean isConnected() {
      return connected;
    }
  }

  static class TestALW implements AsyncLockWatcher {

    LockLossReason reason = null;
    boolean locked = false;
    Exception exception = null;
    int changes = 0;

    @Override
    public synchronized void lostLock(LockLossReason reason) {
      this.reason = reason;
      changes++;
      this.notifyAll();
    }

    @Override
    public synchronized void acquiredLock() {
      this.locked = true;
      changes++;
      this.notifyAll();
    }

    @Override
    public synchronized void failedToAcquireLock(Exception e) {
      this.exception = e;
      changes++;
      this.notifyAll();
    }

    public synchronized void waitForChanges(int numExpected) throws InterruptedException {
      while (changes < numExpected) {
        this.wait();
      }
    }

    @Override
    public synchronized void unableToMonitorLockNode(Throwable e) {
      changes++;
      this.notifyAll();
    }
  }

  @BeforeClass
  public static void setupMiniCluster() throws Exception {

    folder.create();

    accumulo = new MiniAccumuloCluster(folder.getRoot(), "superSecret");

    accumulo.start();

  }

  private static final AtomicInteger pdCount = new AtomicInteger(0);

  @Test(timeout = 10000)
  public void testDeleteParent() throws Exception {
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ZooLock zl =
        new ZooLock(accumulo.getZooKeepers(), 30000, "digest", "secret".getBytes(), parent);

    assertFalse(zl.isLocked());

    ZooReaderWriter zk =
        ZooReaderWriter.getInstance(accumulo.getZooKeepers(), 30000, "digest", "secret".getBytes());

    // intentionally created parent after lock
    zk.mkdirs(parent);

    zk.delete(parent, -1);

    zk.mkdirs(parent);

    TestALW lw = new TestALW();

    zl.lockAsync(lw, "test1".getBytes());

    lw.waitForChanges(1);

    assertTrue(lw.locked);
    assertTrue(zl.isLocked());
    assertNull(lw.exception);
    assertNull(lw.reason);

    zl.unlock();
  }

  @Test(timeout = 10000)
  public void testNoParent() throws Exception {
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ZooLock zl =
        new ZooLock(accumulo.getZooKeepers(), 30000, "digest", "secret".getBytes(), parent);

    assertFalse(zl.isLocked());

    TestALW lw = new TestALW();

    zl.lockAsync(lw, "test1".getBytes());

    lw.waitForChanges(1);

    assertFalse(lw.locked);
    assertFalse(zl.isLocked());
    assertNotNull(lw.exception);
    assertNull(lw.reason);
  }

  @Test(timeout = 10000)
  public void testDeleteLock() throws Exception {
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ZooReaderWriter zk =
        ZooReaderWriter.getInstance(accumulo.getZooKeepers(), 30000, "digest", "secret".getBytes());
    zk.mkdirs(parent);

    ZooLock zl =
        new ZooLock(accumulo.getZooKeepers(), 30000, "digest", "secret".getBytes(), parent);

    assertFalse(zl.isLocked());

    TestALW lw = new TestALW();

    zl.lockAsync(lw, "test1".getBytes());

    lw.waitForChanges(1);

    assertTrue(lw.locked);
    assertTrue(zl.isLocked());
    assertNull(lw.exception);
    assertNull(lw.reason);

    zk.delete(zl.getLockPath(), -1);

    lw.waitForChanges(2);

    assertEquals(LockLossReason.LOCK_DELETED, lw.reason);
    assertNull(lw.exception);

  }

  @Test(timeout = 10000)
  public void testDeleteWaiting() throws Exception {
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ZooReaderWriter zk =
        ZooReaderWriter.getInstance(accumulo.getZooKeepers(), 30000, "digest", "secret".getBytes());
    zk.mkdirs(parent);

    ZooLock zl =
        new ZooLock(accumulo.getZooKeepers(), 30000, "digest", "secret".getBytes(), parent);

    assertFalse(zl.isLocked());

    TestALW lw = new TestALW();

    zl.lockAsync(lw, "test1".getBytes());

    lw.waitForChanges(1);

    assertTrue(lw.locked);
    assertTrue(zl.isLocked());
    assertNull(lw.exception);
    assertNull(lw.reason);

    ZooLock zl2 =
        new ZooLock(accumulo.getZooKeepers(), 30000, "digest", "secret".getBytes(), parent);

    TestALW lw2 = new TestALW();

    zl2.lockAsync(lw2, "test2".getBytes());

    assertFalse(lw2.locked);
    assertFalse(zl2.isLocked());

    ZooLock zl3 =
        new ZooLock(accumulo.getZooKeepers(), 30000, "digest", "secret".getBytes(), parent);

    TestALW lw3 = new TestALW();

    zl3.lockAsync(lw3, "test3".getBytes());

    List<String> children = zk.getChildren(parent);
    Collections.sort(children);

    zk.delete(parent + "/" + children.get(1), -1);

    lw2.waitForChanges(1);

    assertFalse(lw2.locked);
    assertNotNull(lw2.exception);
    assertNull(lw2.reason);

    zk.delete(parent + "/" + children.get(0), -1);

    lw.waitForChanges(2);

    assertEquals(LockLossReason.LOCK_DELETED, lw.reason);
    assertNull(lw.exception);

    lw3.waitForChanges(1);

    assertTrue(lw3.locked);
    assertTrue(zl3.isLocked());
    assertNull(lw3.exception);
    assertNull(lw3.reason);

    zl3.unlock();

  }

  @Test(timeout = 10000)
  public void testUnexpectedEvent() throws Exception {
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ConnectedWatcher watcher = new ConnectedWatcher();
    ZooKeeper zk = new ZooKeeper(accumulo.getZooKeepers(), 30000, watcher);
    zk.addAuthInfo("digest", "secret".getBytes());

    while (!watcher.isConnected()) {
      Thread.sleep(200);
    }

    zk.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    ZooLock zl =
        new ZooLock(accumulo.getZooKeepers(), 30000, "digest", "secret".getBytes(), parent);

    assertFalse(zl.isLocked());

    // would not expect data to be set on this node, but it should not cause problems.....
    zk.setData(parent, "foo".getBytes(), -1);

    TestALW lw = new TestALW();

    zl.lockAsync(lw, "test1".getBytes());

    lw.waitForChanges(1);

    assertTrue(lw.locked);
    assertTrue(zl.isLocked());
    assertNull(lw.exception);
    assertNull(lw.reason);

    // would not expect data to be set on this node either
    zk.setData(zl.getLockPath(), "bar".getBytes(), -1);

    zk.delete(zl.getLockPath(), -1);

    lw.waitForChanges(2);

    assertEquals(LockLossReason.LOCK_DELETED, lw.reason);
    assertNull(lw.exception);

  }

  @Test(timeout = 10000)
  public void testTryLock() throws Exception {
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount.incrementAndGet();

    ZooLock zl = new ZooLock(accumulo.getZooKeepers(), 1000, "digest", "secret".getBytes(), parent);

    ConnectedWatcher watcher = new ConnectedWatcher();
    ZooKeeper zk = new ZooKeeper(accumulo.getZooKeepers(), 1000, watcher);
    zk.addAuthInfo("digest", "secret".getBytes());

    while (!watcher.isConnected()) {
      Thread.sleep(200);
    }

    for (int i = 0; i < 10; i++) {
      zk.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      zk.delete(parent, -1);
    }

    zk.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    TestALW lw = new TestALW();

    boolean ret = zl.tryLock(lw, "test1".getBytes());

    assertTrue(ret);

    // make sure still watching parent even though a lot of events occurred for the parent
    synchronized (zl) {
      Field field = zl.getClass().getDeclaredField("watchingParent");
      field.setAccessible(true);
      assertTrue((Boolean) field.get(zl));
    }

    zl.unlock();
  }

  @Test(timeout = 10000)
  public void testChangeData() throws Exception {
    String parent = "/zltest-" + this.hashCode() + "-l" + pdCount.incrementAndGet();
    ConnectedWatcher watcher = new ConnectedWatcher();
    ZooKeeper zk = new ZooKeeper(accumulo.getZooKeepers(), 1000, watcher);
    zk.addAuthInfo("digest", "secret".getBytes());

    while (!watcher.isConnected()) {
      Thread.sleep(200);
    }

    zk.create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    ZooLock zl = new ZooLock(accumulo.getZooKeepers(), 1000, "digest", "secret".getBytes(), parent);

    TestALW lw = new TestALW();

    zl.lockAsync(lw, "test1".getBytes());
    assertEquals("test1", new String(zk.getData(zl.getLockPath(), null, null)));

    zl.replaceLockData("test2".getBytes());
    assertEquals("test2", new String(zk.getData(zl.getLockPath(), null, null)));
  }

  @AfterClass
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
    folder.delete();
  }

  @Test
  public void x() throws Exception {

    long start = System.nanoTime();
    // com.google.common.base.StopWatch sw = StopWatch.createStarted();

    Random r = SecureRandom.getInstance("SHA1PRNG");

    long l = r.nextLong() & (Long.MAX_VALUE);

    String s = Long.toString(l, Character.MAX_RADIX);

    System.out.println("L: " + s);

    System.out.println("L: " + String.format("%08x", r.nextInt()));

    String prefix = "xlock";
    int eid = 0;

    List<ZEName> names = new ArrayList();
    names.add(new ZEName(String.format("%s/%s-%08x-%010d", "/root/path", prefix, r.nextInt(), eid++)));
    names.add(new ZEName(String.format("%s/%s-%08x-%010d", "/root/path", prefix, r.nextInt(), eid++)));
    names.add(new ZEName(String.format("%s/%s-%08x-%010d", "/root/path", prefix, r.nextInt(), eid++)));
    names.add(new ZEName(String.format("%s/%s-%08x-%010d", "/root/path", prefix, r.nextInt(), eid++)));

    Collections.<ZEName>sort(names, ZEName::compareTo);

    System.out.println("A: " + names);

    System.out.println("Duration: " + Duration.ofNanos(System.nanoTime() - start).toString());
  }

  static class ZEName implements Comparable<ZEName>{

    String fullPath;
    String root;
    String nodePrefix;
    String randId;
    String sequence;

    ZEName(final String fullPath){
      this.fullPath = fullPath;
    }
    private void parse(){
      int lastSlash = fullPath.lastIndexOf('/');

      root = fullPath.substring(1, lastSlash);

      String[] parts = fullPath.substring(lastSlash+1, fullPath.length()).split("-");

      nodePrefix = parts[0];
      randId = parts[1];
      sequence = parts[2];
    }

    public String getRoot() {
      return root;
    }

    public String getNodePrefix() {
      return nodePrefix;
    }

    public String getRandId() {
      return randId;
    }

    public String getSequence() {
      return sequence;
    }

    private static final Comparator<ZEName> bySeq = Comparator.comparing(ZEName::getSequence)
        .thenComparing(ZEName::getRandId);

    private static int compareBySeqId(String lPath, String rPath){
      int lSeq = lPath.lastIndexOf('-');
      if(lSeq == -1){
        throw new IllegalArgumentException("Left side, not a valid ephemeral sequence '" + lPath +"'");
      }

      int rSeq = rPath.lastIndexOf('-');
      if(rSeq == -1){
        throw new IllegalArgumentException("Right side, not a valid ephemeral sequence '" + lPath +"'");
      }

      int r = lPath.substring(lSeq + 1, lPath.length()).compareTo(rPath.substring(rSeq + 1, rPath.length()));

      if(r != 0){
        return r;
      }

      return lPath.compareTo(rPath);

    }
    /**
     * Sort by the sequence number, lowest sequence first.
     * @param other
     * @return -1, 0, 1 if lexicographically less, equal or greater.
     */
    @Override public int compareTo(ZEName other) {
      return compareBySeqId(this.fullPath, other.fullPath);
    }

    @Override public String toString() {
      return "ZEName{" + "fullPath='" + fullPath + '\'' + ", root='" + root + '\'' + ", nodePrefix='" + nodePrefix + '\'' + ", randId='" + randId + '\'' + ", sequence='" + sequence + '\'' + '}';
    }
  }
}
