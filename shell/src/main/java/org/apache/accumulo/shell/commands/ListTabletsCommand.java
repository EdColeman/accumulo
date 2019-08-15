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
package org.apache.accumulo.shell.commands;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.NumUtil;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copied from ACCUMULO-2873 It would be very useful to have a utility that generates single line
 * tablet info. The output of this could be fed to sort, awk, grep, etc inorder to answer questions
 * like which tablets have the most files.The output could look something like the following
 *
 * <pre>
 * $accumulo admin listTablets --table bigTable3
 * #files #walogs #entries #size #status #location #tableid #endrow
 * 6 2 40,001 50M ASSIGNED 10.1.9.9 4:9997[abc]  3 admin
 * 3 1 50,002 40M ASSIGNED 10.1.9.9 5:9997[abc]  3 helpful
 * </pre>
 * <p>
 * All of the information can be obtained by scanning the metadata table and looking into zookeeper.
 * Could possibly contact tablet servers to get info about entries in memory. The order of the
 * columns in the example above is arbitrary, except for end row. Maybe end row column should come
 * last because it can be of arbitrary length. Also the end row could contain any character, could
 * look into using a CSV library. It would be nice to design the utility so that columns can be
 * added in future versions w/o impacting current scripts that use the utility.
 */
public class ListTabletsCommand extends Command {

  private static final Logger log = LoggerFactory.getLogger(ListTabletsCommand.class);

  // default auths.
  private final Authorizations auth = new Authorizations();

  private Option outputFileOpt;
  private Option optTablePattern;
  private Option optHumanReadable;
  private Option optNamespace;
  private Option disablePaginationOpt;

  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {

    final Set<String> tablenames = readTargetTables(cl, shellState);

    try {

      final Connector connector = shellState.getConnector();

      Map<String,String> idMap = connector.tableOperations().tableIdMap();

      List<String> lines = new LinkedList<>();
      String header = "table_name, #files, #wals, #entries, size, status, location, id, end_row";
      lines.add(header);

      boolean humanReadable = cl.hasOption(optHumanReadable.getOpt());

      for (String tablename : tablenames) {

        String tableId = idMap.get(tablename);

        if (tableId == null) {

          TabletRowInfo.Factory factory = new TabletRowInfo.Factory(tablename);
          lines.add(factory.build().format(humanReadable));

        } else {

          List<TabletRowInfo> rows = getTabletRowInfo(tablename, tableId, connector);

          for (TabletRowInfo row : rows) {
            lines.add(row.format(humanReadable));
          }
        }
      }

      if (lines.size() == 1) {
        lines.add("No data");
      }

      if (cl.hasOption(outputFileOpt.getOpt())) {
        final String outputFile = cl.getOptionValue(outputFileOpt.getOpt());
        Shell.PrintFile printFile = new Shell.PrintFile(outputFile);
        shellState.printLines(lines.iterator(), false, printFile);
      } else {
        boolean paginate = !cl.hasOption(disablePaginationOpt.getOpt());
        shellState.printLines(lines.iterator(), paginate);
      }

    } catch (Exception ex) {
      ex.printStackTrace();
      throw new RuntimeException(ex);
    }
    return 0;
  }

  /**
   * Process the command line for table names using table option, table name pattern, or default to
   * current table.
   *
   * @param cl
   *          command line
   * @param shellState
   *          shell state
   * @return set of table names.
   * @throws NamespaceNotFoundException
   *           if the namespace option is specified and namespace does not exist
   */
  private Set<String> readTargetTables(final CommandLine cl, final Shell shellState)
      throws NamespaceNotFoundException {

    Set<String> tablenames = new TreeSet<>();

    if (cl.hasOption(ShellOptions.tableOption)) {
      tablenames.add(cl.getOptionValue(ShellOptions.tableOption));
    }

    if (cl.hasOption(optNamespace.getOpt())) {
      Instance instance = shellState.getInstance();
      String namespaceId =
          Namespaces.getNamespaceId(instance, cl.getOptionValue(optNamespace.getOpt()));
      tablenames.addAll(Namespaces.getTableNames(instance, namespaceId));
    }

    // Add any patterns
    if (cl.hasOption(optTablePattern.getOpt())) {
      for (String table : shellState.getConnector().tableOperations().list()) {
        if (table.matches(cl.getOptionValue(optTablePattern.getOpt()))) {
          tablenames.add(table);
        }
      }
    }

    // If we didn't get any tables, and we have a table selected, add the current table
    if (tablenames.isEmpty() && !shellState.getTableName().isEmpty()) {
      tablenames.add(shellState.getTableName());
    }

    return tablenames;

  }

  private List<TabletRowInfo> getTabletRowInfo(String tablename, final String tableId,
      final Connector connector) {

    if (log.isTraceEnabled()) {
      log.trace("get row info for table name {}" + tablename);
    }

    log.debug("scan metadata for tablet info table name: \'{}\', tableId: \'{}\' ", tablename,
        tableId);

    List<TabletRowInfo> tResults = getMetadataInfo(connector, tablename, tableId);

    if (log.isTraceEnabled()) {
      for (TabletRowInfo tabletRowInfo : tResults) {
        log.trace("Tablet info: {}", tabletRowInfo);
      }
    }

    return tResults;
  }

  private Scanner buildScanner(final Connector connector, String tablename, String id)
      throws TableNotFoundException {

    Scanner scanner = connector.createScanner(tablename, auth);

    Range range = MetadataSchema.TabletsSection.getRange(id);
    scanner.setRange(range);

    for (Text cf : TabletRowInfo.COL_FAMILIES) {
      scanner.fetchColumnFamily(cf);
    }

    return scanner;
  }

  private List<TabletRowInfo> getMetadataInfo(Connector connector, String tableName, String id) {

    List<TabletRowInfo> results = new ArrayList<>();

    TabletRowInfo.Factory tabletInfoFactory = new TabletRowInfo.Factory(tableName);

    try (Scanner scanner = buildScanner(connector, tableName, id)) {

      Text currentRow = new Text("");

      boolean firstRowComplete = false;

      for (Map.Entry<Key,Value> entry : scanner) {

        Text row = entry.getKey().getRow();
        Value value = entry.getValue();

        if (row.compareTo(currentRow) != 0) {
          currentRow = row;

          if (!firstRowComplete) {
            results.add(tabletInfoFactory.build());
            firstRowComplete = true;
          }

          tabletInfoFactory = new TabletRowInfo.Factory(tableName);
          tabletInfoFactory.tableId(id);
          tabletInfoFactory.tableExists(true);
          tabletInfoFactory.updateInfo(entry.getKey(), value);
          tabletInfoFactory.endRow(row.toString());

        } else {

          tabletInfoFactory.updateInfo(entry.getKey(), value);
        }
      }
      // emit last row
      results.add(tabletInfoFactory.build());

    } catch (TableNotFoundException ex) {
      ex.printStackTrace();
      TabletRowInfo.Factory factory = new TabletRowInfo.Factory(tableName);
      factory.tableId(id);
      factory.tableExists(false);
      results.add(factory.build());
    }

    return results;
  }

  @Override
  public String description() {
    return "prints tablet info an a single line.  Info contains #files #walogs #entries #size #status #location #tableid #endrow";
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public Options getOptions() {

    final Options opts = new Options();
    opts.addOption(OptUtil.tableOpt("table to be scanned"));

    optTablePattern = new Option("p", "pattern", true, "regex pattern of table names");
    optTablePattern.setArgName("pattern");
    opts.addOption(optTablePattern);

    optNamespace =
        new Option(ShellOptions.namespaceOption, "namespace", true, "name of a namespace");
    optNamespace.setArgName("namespace");
    opts.addOption(optNamespace);

    optHumanReadable =
        new Option("h", "human-readable", false, "format large sizes to human readable units");
    optHumanReadable.setArgName("human readable output");
    opts.addOption(optHumanReadable);

    disablePaginationOpt =
        new Option("np", "no-pagination", false, "disables pagination of output");
    opts.addOption(disablePaginationOpt);

    outputFileOpt = new Option("o", "output", true, "local file to write output to");
    outputFileOpt.setArgName("file");
    opts.addOption(outputFileOpt);

    return opts;
  }

  static class TabletRowInfo {

    private final String tableName;
    private final int numFiles;
    private final int numWalLogs;
    private final long numEntries;
    private final long size;
    private final String status;
    private final String location;
    private final String tableId;
    private final String endRow;
    private final boolean tableExists;

    // debug - dev only.
    private final BigInteger tag = new BigInteger(32, ThreadLocalRandom.current());

    private TabletRowInfo(String tableName, int numFiles, int numWalLogs, long numEntries,
        long size, String status, String location, String tableId, String endRow,
        boolean tableExists) {
      this.tableName = tableName;
      this.numFiles = numFiles;
      this.numWalLogs = numWalLogs;
      this.numEntries = numEntries;
      this.size = size;
      this.status = status;
      this.location = location;
      this.tableId = tableId;
      this.endRow = endRow;
      this.tableExists = tableExists;
    }

    String getTableName() {
      return tableName;
    }

    int getNumFiles() {
      return numFiles;
    }

    int getNumWalLogs() {
      return numWalLogs;
    }

    long getNumEntries() {
      return numEntries;
    }

    String getNumEntries(final boolean humanReadable) {
      if (humanReadable) {
        return String.format("%9s", NumUtil.bigNumberForQuantity(numEntries));
      }
      return String.format("%,24d", numEntries);
    }

    long getSize() {
      return size;
    }

    String getSize(final boolean humanReadable) {
      if (humanReadable) {
        return String.format("%9s", NumUtil.bigNumberForSize(size));
      }
      return String.format("%,24d", size);
    }

    public String getStatus() {

      if (numWalLogs > 0) {
        return "RECOVERING";
      }

      if (location.length() > 0) {
        return "ASSIGNED";
      }
      return "UNASSIGNED";
    }

    public String getLocation() {
      return location;
    }

    public String getTableId() {
      return tableId;
    }

    public String getEndRow() {
      return endRow;
    }

    public boolean tableExists() {
      return tableExists;
    }

    static final Text fileCf = MetadataSchema.TabletsSection.DataFileColumnFamily.NAME;
    static final Text locCf = MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME;
    static final Text logCf = MetadataSchema.TabletsSection.LogColumnFamily.NAME;
    static final Text tabCf = MetadataSchema.TabletsSection.TabletColumnFamily.NAME;

    static final Text[] COL_FAMILIES = {fileCf, locCf, logCf, tabCf};

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("TabletInfo{");
      sb.append("tag=\'").append(String.format("%08x", tag)).append('\'');
      sb.append(", tableName='").append(tableName).append('\'');
      sb.append(", numFiles=").append(numFiles);
      sb.append(", numWalLogs=").append(numWalLogs);
      sb.append(", numEntries=").append(numEntries);
      sb.append(", size=").append(size);
      sb.append(", status='").append(status).append('\'');
      sb.append(", location='").append(location).append('\'');
      sb.append(", tableId='").append(tableId).append('\'');
      sb.append(", endRow='").append(endRow).append('\'');
      sb.append(", tableExists=").append(tableExists);
      sb.append('}');
      return sb.toString();
    }

    String format(boolean prettyPrint) {
      return String.format("%s, %d, %d, %s, %s, %s, %s, %s, %s", getTableName(), getNumFiles(),
          getNumWalLogs(), getNumEntries(prettyPrint), getSize(prettyPrint), getStatus(),
          getLocation(), getTableId(), getEndRow());
    }

    public static class Factory {
      final String tableName;
      int numFiles = 0;
      int numWalLogs = 0;
      long numEntries = 0;
      long size = 0;
      String status = "";
      String location = "";
      String tableId = "";
      String endRow = "";
      boolean tableExists = false;
      int parseErrors = 0;
      final List<Throwable> exceptions = new ArrayList<>();

      Factory(final String tableName) {
        this.tableName = tableName;
      }

      Factory numFiles(int numFiles) {
        this.numFiles = numFiles;
        return this;
      }

      Factory numWalLogs(int numWalLogs) {
        this.numWalLogs = numWalLogs;
        return this;
      }

      public Factory numEntries(long numEntries) {
        this.numEntries = numEntries;
        return this;
      }

      public Factory size(int size) {
        this.size = size;
        return this;
      }

      public Factory status(String status) {
        this.status = status;
        return this;
      }

      public Factory location(String location) {
        this.location = location;
        return this;
      }

      public Factory tableId(String tableId) {
        this.tableId = tableId;
        return this;
      }

      private static final Pattern rowPattern = Pattern.compile("^\\s*(.+)([;<])(.*)");

      public Factory endRow(String endRow) {
        Matcher m = rowPattern.matcher(endRow);
        if (m.matches()) {
          if (m.group(1).compareTo(tableId) != 0) {
            this.endRow = String.format("ERR: %s", endRow);
            return this;
          }
          if (m.group(2).compareTo("<") == 0) {
            this.endRow = "+INF";
            return this;
          } else {
            this.endRow = m.group(3);
            return this;
          }
        }
        this.endRow = String.format("UNK: %s", endRow);
        return this;
      }

      public Factory tableExists(boolean tableExists) {
        this.tableExists = tableExists;
        return this;
      }

      void updateInfo(final Key key, final Value value) {
        Text cf = key.getColumnFamily();

        try {
          if (cf.compareTo(fileCf) == 0) {
            numFiles += 1;
            String[] tokens = value.toString().split(",");
            if (tokens.length == 2) {
              size += Long.parseLong(tokens[0]);
              numEntries += Long.parseLong(tokens[1]);
            }
          }

          if (cf.compareTo(locCf) == 0) {
            location = value.toString();

          }

          if (cf.compareTo(logCf) == 0) {
            numWalLogs++;

          }

        } catch (NumberFormatException ex) {
          parseErrors++;
          exceptions.add(ex);
        }
      }

      public TabletRowInfo build() {
        return new TabletRowInfo(tableName, numFiles, numWalLogs, numEntries, size, status,
            location, tableId, endRow, tableExists);
      }
    }
  }

}
