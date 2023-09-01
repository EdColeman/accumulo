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

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.tserver.logger.DumpWalTransaction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WalToolsIT extends ConfigurableMacBase {
  private static final Logger LOG = LoggerFactory.getLogger(WalToolsIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(2);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  /**
   * set 'accumulo.properties' system property to props used to launch cluster.
   */
  @BeforeEach
  public void configPropsFromCluster() {
    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
  }

  @Test
  public void fsTest() throws Exception {
    File accumuloDir = cluster.getConfig().getAccumuloDir();
    var volMgr = getServerContext().getVolumeManager();
    Path p = new Path(accumuloDir.getAbsolutePath());
    // + "/instance_id/" + cluster.getServerContext().getInstanceID());
    LOG.info("FS: STAT {}", volMgr.getFileStatus(p));
    var itor = volMgr.listFiles(p, true);
    try {
      while (itor.hasNext()) {
        LOG.info("F: {}", itor.next());
      }
    } catch (Exception ex) {
      LOG.info("skipping");
    }
  }

  @Test
  public void processTable1Test() throws Exception {
    String[] names = getUniqueNames(2);
    try (AccumuloClient client = Accumulo.newClient().from(cluster.getClientProperties()).build()) {
      client.tableOperations().compact("accumulo.metadata", new CompactionConfig().setWait(true));
      // generate metadata activity to populate wals.
      SortedSet<Text> splits = new TreeSet<>(List.of(new Text("c"), new Text("k"), new Text("p")));
      var ntc = new NewTableConfiguration().withSplits(splits);
      client.tableOperations().create(names[0], ntc);
      client.tableOperations().create(names[1], ntc);

      LOG.warn("CLIENT: SITE: {}", client.instanceOperations().getSiteConfiguration());
      LOG.warn("CLIENT: SYSTEM: {}", client.instanceOperations().getSystemConfiguration());
    }

    Map<String,String> siteProps = new TreeMap<>();
    getCluster().getSiteConfiguration().getProperties(siteProps, (x) -> true);

    LOG.info("SITE: {}", siteProps);

    LOG.info("PROPS: {}", cluster.getClientProperties());

    Map<String,String> contextProps = new TreeMap<>();
    getCluster().getServerContext().getConfiguration().getProperties(contextProps, (x) -> true);
    LOG.info("SC PROPS: {}", siteProps);

    fsTest();

    DumpWalTransaction walTool = new DumpWalTransaction();
    walTool.execute(List.of("-t", names[0]).toArray(new String[0]));

  }

  @Test
  public void scanTest() throws Exception {
    String[] names = getUniqueNames(2);
    Properties props = cluster.getClientProperties();
    try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
      client.tableOperations().compact("accumulo.metadata", new CompactionConfig().setWait(true));
      // generate metadata activity to populate wals.
      SortedSet<Text> splits = new TreeSet<>(List.of(new Text("c"), new Text("k"), new Text("p")));
      var ntc = new NewTableConfiguration().withSplits(splits);
      client.tableOperations().create(names[0], ntc);
      client.tableOperations().create(names[1], ntc);

      // ROOT scanner
      try (Scanner meta = client.createScanner(RootTable.NAME, Authorizations.EMPTY)) {
        String tableId = client.tableOperations().tableIdMap().get("accumulo.metadata");
        LOG.info("ROOT TABLE ID: {}", tableId);
        meta.setRange(new Range(new Text(tableId + ";"), new Text(tableId + "<")));
        meta.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
        for (Map.Entry<Key,Value> entry : meta) {
          LOG.info("ROOT: {}", entry);
        }
      }
      // Metadata scanner
      try (Scanner meta = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        String tableId = client.tableOperations().tableIdMap().get(names[0]);
        LOG.info("META TABLE ID: {}", tableId);
        meta.setRange(new Range(new Text(tableId + ";"), new Text(tableId + "<")));
        meta.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
        for (Map.Entry<Key,Value> entry : meta) {
          LOG.info("META: {} -> {}", entry.getKey(), entry.getValue());
        }
      }
      FileSystem fs = cluster.getFileSystem();
      Path p = new Path(cluster.getConfig().getAccumuloDir().getAbsolutePath());
      LOG.warn("FS TEST: {}", fs.exists(p));
      LOG.warn("VM TEST: {}", getServerContext().getVolumeManager().exists(p));

    }
  }

  @Test
  public void x() {
    LOG.info("splits: {}", new TreeSet<>(Arrays.asList(new Text("efg"))));
  }
}
