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
package org.apache.accumulo.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.metrics.TestStatsDRegistryFactory;
import org.apache.accumulo.test.metrics.TestStatsDSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BadFileIT extends SharedMiniClusterBase {

  private static final Logger LOG = LoggerFactory.getLogger(BadFileIT.class);

  private static TestStatsDSink sink;
  private static Thread metricConsumer;

  public static class TestConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.setNumTservers(2);
      cfg.setMemory(ServerType.TABLET_SERVER, 256, MemoryUnit.MEGABYTE);
      cfg.setProperty(Property.GENERAL_MICROMETER_ENABLED, "true");
      cfg.setProperty(Property.GENERAL_MICROMETER_FACTORY,
          TestStatsDRegistryFactory.class.getName());
      Map<String,String> sysProps = Map.of(TestStatsDRegistryFactory.SERVER_HOST, "127.0.0.1",
          TestStatsDRegistryFactory.SERVER_PORT, Integer.toString(sink.getPort()));
      cfg.setSystemProperties(sysProps);
    }
  }

  @BeforeAll
  public static void start() throws Exception {
    sink = new TestStatsDSink();
    metricConsumer = new Thread(() -> {
      while (!Thread.currentThread().isInterrupted()) {
        List<String> statsDMetrics = sink.getLines();
        for (String line : statsDMetrics) {
          if (Thread.currentThread().isInterrupted()) {
            break;
          }
          if (line.startsWith("accumulo")) {
            // TestStatsDSink.Metric metric = TestStatsDSink.parseStatsDMetric(line);
            LOG.trace("metrics line: {}", line);
          }
        }
      }
    });
    metricConsumer.start();

    SharedMiniClusterBase.startMiniClusterWithConfig(new TestConfiguration());
  }

  @AfterAll
  public static void stop() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
    sink.close();
    metricConsumer.interrupt();
    metricConsumer.join();
  }

  @Test
  public void truncatedUserTableFileTest() throws Exception {

    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      TableOperations to = client.tableOperations();
      NewTableConfiguration ntc = new NewTableConfiguration()
          .withSplits(new TreeSet<>(List.of(new Text("row_0000000049"))));
      to.create(table, ntc);

      // pause for migrations
      Thread.sleep(SECONDS.toMillis(1));

      ReadWriteIT.ingest(client, 100, 2, 10, 0, table);

      to.compact(table, new CompactionConfig().setWait(true));
      to.compact("accumulo.metadata", new CompactionConfig().setWait(true));
      to.compact("accumulo.root", new CompactionConfig().setWait(true));

      long rowCount = getRowCount(table, client);

      LOG.info("RRRRR row count {}", rowCount);

      Path sourceFile = pickFile(table, client);
      Path truncatedFile = truncateFile(sourceFile);

      LOG.info("CREATED: {} - {}",
          getCluster().getFileSystem().getFileStatus(truncatedFile).getLen(), truncatedFile);

      to.offline(table, true);

      swapFiles(sourceFile, truncatedFile);

      to.online(table, true);

      // to.compact(table, new CompactionConfig().setWait(true));

      long errorCount = getRowCount(table, client);

      assertNotEquals(rowCount, errorCount);
    }
  }

  private static long getRowCount(String table, AccumuloClient client) throws Exception {
    long rowCount;
    try (Scanner scanner = client.createScanner(table)) {
      rowCount = scanner.stream().count();
    }
    return rowCount;
  }

  private Path pickFile(final String tableName, final AccumuloClient client) {

    ServerContext context = getCluster().getServerContext();
    TableOperations to = client.tableOperations();
    TableId tid = TableId.of(to.tableIdMap().get(tableName));
    List<Path> paths = new ArrayList<>();
    try (TabletsMetadata tablets =
        context.getAmple().readTablets().forTable(tid).fetch(FILES).build()) {
      tablets.forEach(tm -> tm.getFiles().forEach(f -> paths.add(f.getPath())));
    }
    LOG.info("AMPLE: R: {}", paths);
    return paths.get(0);
  }

  private Path truncateFile(final Path inFile) throws Exception {
    // use small chuck whole file will have multiple chunks
    final int chunk_size = 8;
    final double filePercent = 0.50;

    FileStatus fileStatus = getCluster().getFileSystem().getFileStatus(inFile);
    long originalSize = fileStatus.getLen();

    FileSystem fs = getCluster().getFileSystem();
    Path outFile = new Path(inFile.toUri() + ".trunc");
    try (InputStream in = new BufferedInputStream(fs.open(inFile));
        OutputStream out = new BufferedOutputStream(fs.create(outFile))) {
      byte[] buffer = new byte[chunk_size];
      int lengthRead;
      long totalWritten = 0;
      while ((lengthRead = in.read(buffer)) > 0) {
        totalWritten += lengthRead;
        // stop copy when percent threshold reached.
        if (totalWritten > (originalSize * filePercent)) {
          break;
        }
        out.write(buffer, 0, lengthRead);
        out.flush();
      }
    }
    return outFile;
  }

  private void swapFiles(final Path path1, final Path path2) throws Exception {
    FileSystem fs = getCluster().getFileSystem();
    Path tmp = new Path(path1.toString() + ".tmp");
    LOG.warn("Rename {} to {}", path1.getName(), tmp.getName());
    fs.rename(path1, tmp);
    LOG.warn("Rename {} to {}", path2.getName(), path1.getName());
    fs.rename(path2, path1);
    LOG.warn("Rename {} to {}", tmp.getName(), path2.getName());
    fs.rename(tmp, path2);
  }
}
