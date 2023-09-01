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
package org.apache.accumulo.tserver.logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class DumpWalTransaction implements KeywordExecutable {

  private static final Logger LOG = LoggerFactory.getLogger(DumpWalTransaction.class);

  private final Opts opts = new Opts();

  static class Opts extends ConfigOpts {
    @Parameter(names = {"-a", "--archive-only"}, description = "copy wals for later analysis")
    Boolean archiveOnly = false;
    @Parameter(names = {"-t", "--tableNames"},
        description = "process wals for given tables - defaults to root and metadata")
    List<String> tables = List.of("accumulo.root", "accumulo.metadata");
    @Parameter(names = "-r", description = "print only mutations associated with the given row")
    String row;
    @Parameter(names = {"-e", "--keyExtent"},
        description = "print only mutations that fall within the given key extent")
    String extent;
    @Parameter(names = "--regex", description = "search for a row that matches the given regex")
    String regexp;
    @Parameter(names = "--accumuloRootDir",
        description = "testing only - provide an path to accumulo files, use only if not set in context")
    String accumuloRootDir = "/accumulo";
    @Parameter(names = "-f", description = "list of wal logs to process")
    List<String> files = new ArrayList<>();
  }

  @Override
  public String keyword() {
    return "wal-transaction-info";
  }

  @Override
  public String description() {
    return "process wals files and provide user-readable transaction record for debugging";
  }

  @Override
  public void execute(String[] args) throws Exception {

    opts.parseArgs(DumpWalTransaction.class.getName(), args);
    var siteConfig = opts.getSiteConfiguration();

    try (ServerContext context = new ServerContext(siteConfig)) {

      VolumeManager vm = context.getVolumeManager();
      var chooserEnv = new VolumeChooserEnvironmentImpl(
          org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment.Scope.LOGGER, context);
      String logPath =
          vm.choose(chooserEnv, context.getBaseUris()) + Path.SEPARATOR + Constants.WAL_DIR;
      // + Path.SEPARATOR + logger + Path.SEPARATOR + filename;

      LOG.info("ACCUMULO REV : {}", context.getRecoveryDirs());
      LOG.info("ACCUMULO BASE: {}", context.getBaseUris());
      LOG.info("ACCUMULO LOGP: {}", logPath);

      // LOG.info("FS: {}", vm.getFileStatus(new
      // Path(context.getBaseUris().stream().findFirst().orElse(opts.accumuloRootDir))));

      try (AccumuloClient client = Accumulo.newClient().from(context.getProperties()).build()) {
        LOG.info("Have accumulo client: {}", client);
        LOG.info("tables: {}", client.tableOperations().tableIdMap());
      }

      LOG.warn("options table names: {}", opts.tables);

      metadataScanner(context.getProperties(), opts.tables.get(0));
    }
  }

  private void metadataScanner(final Properties props, final String tableName)
      throws TableNotFoundException {
    LOG.info("METASCAN: Table name: {}", tableName);
    try (AccumuloClient client = Accumulo.newClient().from(props).build()) {

      try (Scanner meta = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        String tableId = client.tableOperations().tableIdMap().get(tableName);
        LOG.info("METASCAN: TABLE IF: {}", tableId);
        meta.setRange(new Range(new Text(tableId + ";"), new Text(tableId + "<")));
        meta.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
        for (Map.Entry<Key,Value> entry : meta) {
          LOG.info("META: {}", entry);
        }
      }
    }
  }

  private void rootScanner(final Properties props) throws TableNotFoundException {

    try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
      // ROOT scanner
      try (Scanner meta = client.createScanner(RootTable.NAME, Authorizations.EMPTY)) {
        String tableId = client.tableOperations().tableIdMap().get("accumulo.metadata");
        LOG.info("TABLE IF: {}", tableId);
        meta.setRange(new Range(new Text(tableId + ";"), new Text(tableId + "<")));
        meta.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
        for (Map.Entry<Key,Value> entry : meta) {
          LOG.info("ROOT: {}", entry);
        }
      }
    }
  }
}
