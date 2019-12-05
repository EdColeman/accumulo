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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.StringJoiner;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.zookeeper.ZooReaderWriterFactory;
import org.apache.accumulo.test.util.SlowOps;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple test to exercise setting system and table properties to determine zookeeper watch
 * notifications on the .../config/ node in zookeeper.
 */
public class ConfigWatcherDemoIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(ConfigWatcherDemoIT.class);

  private AccumuloClient client;
  private ClientContext context;

  private String tableName;

  private String secret;

  private long maxWait;

  private SlowOps slowOps;

  @Before
  public void setup() {

    client = Accumulo.newClient().from(getClientProps()).build();
    context = (ClientContext) client;

    tableName = getUniqueNames(1)[0];

    secret = cluster.getSiteConfiguration().get(Property.INSTANCE_SECRET);

    maxWait = defaultTimeoutSeconds() <= 0 ? 60_000 : ((defaultTimeoutSeconds() * 1000) / 2);

    slowOps = new SlowOps(client, tableName, maxWait, 1);
  }

  @After
  public void closeClient() {
    client.close();
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  /**
   * Manually set watches on the system prop config directory and validate assumptions.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void setSystemPropsTest() throws Exception {

    String instanceId = context.getInstanceID();

    IZooReaderWriter zk = new ZooReaderWriterFactory().getZooReaderWriter(context.getZooKeepers(),
        context.getZooKeepersSessionTimeOut(), secret);

    var zConfigPath = Constants.ZROOT + "/" + instanceId + Constants.ZCONFIG;

    boolean found = zk.exists(zConfigPath);
    if (!found) {
      throw new IllegalStateException(
          "Failed - required " + zConfigPath + " to exist in zookeeper");
    }

    LogWatcher eventWatcher = new LogWatcher();
    found = zk.exists(zConfigPath, eventWatcher);

    LogWatcher childWatcher = new LogWatcher();
    zk.getChildren(zConfigPath, childWatcher);

    Stat statBeforeChanges = new Stat();
    byte[] data = zk.getData(zConfigPath, statBeforeChanges);
    log.debug("Data length {} for {}", data.length, prettyStat(statBeforeChanges));

    // validate assumption that data is initialized with new byte[0]
    assertEquals(0, data.length);

    // with exists and getChildren watchers set, make some configuration changes.

    client.tableOperations().setProperty(tableName, Property.TABLE_FILE_MAX.getKey(), "10");
    client.instanceOperations().setProperty(Property.TABLE_BLOOM_ENABLED.getKey(), "true");

    Stat statAfterAdds = new Stat();
    byte[] data2 = zk.getData(zConfigPath, statAfterAdds);
    log.debug("Data length {} for {}", data2.length, prettyStat(statAfterAdds));

    // validate assumption that data length has not changed.
    assertEquals("verify data length has not changed", data.length, data2.length);
    assertEquals("verify data version has not changed", statBeforeChanges.getVersion(),
        statAfterAdds.getVersion());
    assertTrue("verify child version has increased",
        statBeforeChanges.getCversion() < statAfterAdds.getCversion());
    assertTrue("verify number of children increased",
        statBeforeChanges.getNumChildren() < statAfterAdds.getNumChildren());

    log.debug("Event watcher after additions: {}", eventWatcher);
    log.debug("Child watcher after additions: {}", childWatcher);

    assertEquals("validate that data watcher has not fired", 0, eventWatcher.getTriggerCount());
    assertEquals("validate child watcher fired once", 1, childWatcher.getTriggerCount());

    assertTrue("validate the child event was a node change",
        childWatcher.getLastEvent().getType().equals(Watcher.Event.EventType.NodeChildrenChanged));

    // reset the child watcher.
    zk.getChildren(zConfigPath, childWatcher);

    client.instanceOperations().removeProperty(Property.TABLE_BLOOM_ENABLED.getKey());

    log.debug("Event watcher after delete: {}", eventWatcher);
    log.debug("Child watcher after delete: {}", childWatcher);

    Stat statAfterDelete = new Stat();
    byte[] data3 = zk.getData(zConfigPath, statAfterDelete);

    assertEquals("validate that data watcher has not fired", 0, eventWatcher.getTriggerCount());
    assertEquals("validate child watcher fired twice", 2, childWatcher.getTriggerCount());

    assertEquals("verify data length has not changed", data2.length, data3.length);
    assertEquals("verify data version has not changed", statAfterAdds.getVersion(),
        statAfterDelete.getVersion());
    assertTrue("verify child version has increased",
        statAfterAdds.getCversion() < statAfterDelete.getCversion());
    assertTrue("verify number of children decreased",
        statAfterAdds.getNumChildren() > statAfterDelete.getNumChildren());

  }

  @Test
  public void setTablePropsTest() throws Exception {

    String instanceId = context.getInstanceID();

    IZooReaderWriter zk = new ZooReaderWriterFactory().getZooReaderWriter(context.getZooKeepers(),
        context.getZooKeepersSessionTimeOut(), secret);

    String tableId = client.tableOperations().tableIdMap().get(tableName);

    var zConfigPath = Constants.ZROOT + "/" + instanceId + Constants.ZTABLES + "/" + tableId
        + Constants.ZTABLE_CONF;

    boolean found = zk.exists(zConfigPath);
    if (!found) {
      throw new IllegalStateException(
          "Failed - required " + zConfigPath + " to exist in zookeeper");
    }

    LogWatcher eventWatcher = new LogWatcher();
    found = zk.exists(zConfigPath, eventWatcher);

    LogWatcher childWatcher = new LogWatcher();
    zk.getChildren(zConfigPath, childWatcher);

    Stat statBeforeChanges = new Stat();
    byte[] data = zk.getData(zConfigPath, statBeforeChanges);
    log.debug("Data length {} for {}", data.length, prettyStat(statBeforeChanges));

    // validate assumption that data is initialized with new byte[0]
    assertEquals(0, data.length);

    // with exists and getChildren watchers set, make some configuration changes.

    client.tableOperations().setProperty(tableName, Property.TABLE_FILE_MAX.getKey(), "10");
    client.instanceOperations().setProperty(Property.TABLE_BLOOM_ENABLED.getKey(), "true");

    Stat statAfterAdds = new Stat();
    byte[] data2 = zk.getData(zConfigPath, statAfterAdds);
    log.debug("Data length {} for {}", data2.length, prettyStat(statAfterAdds));

    // validate assumption that data length has not changed.
    assertEquals("verify data length has not changed", data.length, data2.length);
    assertEquals("verify data version has not changed", statBeforeChanges.getVersion(),
        statAfterAdds.getVersion());
    assertTrue("verify child version has increased",
        statBeforeChanges.getCversion() < statAfterAdds.getCversion());
    assertTrue("verify number of children increased",
        statBeforeChanges.getNumChildren() < statAfterAdds.getNumChildren());

    log.debug("Event watcher after additions: {}", eventWatcher);
    log.debug("Child watcher after additions: {}", childWatcher);

    assertEquals("validate that data watcher has not fired", 0, eventWatcher.getTriggerCount());
    assertEquals("validate child watcher fired once", 1, childWatcher.getTriggerCount());

    assertTrue("validate the child event was a node change",
        childWatcher.getLastEvent().getType().equals(Watcher.Event.EventType.NodeChildrenChanged));

    // reset the child watcher.
    zk.getChildren(zConfigPath, childWatcher);

    client.instanceOperations().removeProperty(Property.TABLE_BLOOM_ENABLED.getKey());
    client.tableOperations().removeProperty(tableName, Property.TABLE_FILE_MAX.getKey());

    log.debug("Event watcher after delete: {}", eventWatcher);
    log.debug("Child watcher after delete: {}", childWatcher);

    Stat statAfterDelete = new Stat();
    byte[] data3 = zk.getData(zConfigPath, statAfterDelete);

    assertEquals("validate that data watcher has not fired", 0, eventWatcher.getTriggerCount());
    assertEquals("validate child watcher fired twice", 2, childWatcher.getTriggerCount());

    assertEquals("verify data length has not changed", data2.length, data3.length);
    assertEquals("verify data version has not changed", statAfterAdds.getVersion(),
        statAfterDelete.getVersion());
    assertTrue("verify child version has increased",
        statAfterAdds.getCversion() < statAfterDelete.getCversion());
    assertTrue("verify number of children decreased",
        statAfterAdds.getNumChildren() > statAfterDelete.getNumChildren());

  }

  /**
   * A zookeeper watcher that logs the event, keeps a running count and the last event.
   */
  private static class LogWatcher implements Watcher {

    private int triggerCount = 0;
    private WatchedEvent lastEvent = null;

    @Override
    public void process(WatchedEvent event) {
      log.info("ZK Event: {}", event);
      lastEvent = event;
      triggerCount++;
    }

    public int getTriggerCount() {
      return triggerCount;
    }

    public WatchedEvent getLastEvent() {
      return lastEvent;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", LogWatcher.class.getSimpleName() + "[", "]")
          .add("triggerCount=" + triggerCount).add("lastEvent=" + lastEvent).toString();
    }
  }

  /**
   * Add names to stat variables for readability.
   *
   * @param stat
   *          a zookeeper stat node.
   * @return a print formatted string
   */
  private static String prettyStat(final Stat stat) {
    return new StringJoiner(", ", "Stat[", "]").add("czxid=" + stat.getCzxid())
        .add("mzxid=" + stat.getMzxid()).add("ctime=" + stat.getCtime())
        .add("mtime=" + stat.getMtime()).add("version=" + stat.getVersion())
        .add("cversion=" + stat.getCversion()).add("aversion=" + stat.getAversion())
        .add("ephemeralOwner=" + stat.getEphemeralOwner()).add("dataLength=" + stat.getDataLength())
        .add("numChildren=" + stat.getNumChildren()).add("pzxid=" + stat.getPzxid()).toString();
  }

}
