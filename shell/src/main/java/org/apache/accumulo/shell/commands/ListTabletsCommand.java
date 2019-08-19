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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
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

  private Option outputFileOpt;
  private Option optTablePattern;
  private Option optHumanReadable;
  private Option optNamespace;
  private Option disablePaginationOpt;
  private Option noFlushOption;

  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {

    final Set<TableInfo> tableInfoSet = populateTables(cl, shellState);

    boolean flush = true;

    try {

      final Connector connector = shellState.getConnector();

      List<String> lines = new LinkedList<>();
      String header = "table_name, #files, #wals, #entries, size, status, location, id, end_row";
      lines.add(header);

      boolean humanReadable = cl.hasOption(optHumanReadable.getOpt());

      if (cl.hasOption(noFlushOption.getOpt())) {
        flush = false;
      }

      for (TableInfo tableInfo : tableInfoSet) {

        if (flush) {
          shellState.getConnector().tableOperations().flush(tableInfo.getName(), null, null, true);
        }

        List<TabletRowInfo> rows = getTabletRowInfo(tableInfo, connector);

        for (TabletRowInfo row : rows) {
          lines.add(row.format(humanReadable));
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
  private Set<TableInfo> populateTables(final CommandLine cl, final Shell shellState)
      throws NamespaceNotFoundException, TableNotFoundException {

    final TableOperations tableOps = shellState.getConnector().tableOperations();
    final Instance instance = shellState.getInstance();

    Set<TableInfo> tableSet = new TreeSet<>();

    if (cl.hasOption(optTablePattern.getOpt())) {
      String tablePattern = cl.getOptionValue(optTablePattern.getOpt());
      for (String table : tableOps.list()) {
        if (table.matches(tablePattern)) {
          String id = Tables.getTableId(instance, table);
          tableSet.add(new TableInfo(table, id));
        }
      }
      return validateTableSet(tableSet);
    }

    if (cl.hasOption(optNamespace.getOpt())) {
      String namespaceId =
          Namespaces.getNamespaceId(instance, cl.getOptionValue(optNamespace.getOpt()));
      for (String tableId : Namespaces.getTableIds(instance, namespaceId)) {
        String name = Tables.getTableName(instance, tableId);
        tableSet.add(new TableInfo(name, Tables.getTableId(instance, name)));
      }
      return validateTableSet(tableSet);
    }

    if (cl.hasOption(ShellOptions.tableOption)) {
      String table = cl.getOptionValue(ShellOptions.tableOption);
      String id = Tables.getTableId(instance, table);
      tableSet.add(new TableInfo(table, id));
      return validateTableSet(tableSet);
    }

    // If we didn't get any tables, and we have a table selected, add the current table
    String table = shellState.getTableName();
    if (!table.isEmpty()) {
      String id = Tables.getTableId(instance, table);
      tableSet.add(new TableInfo(table, id));
      return validateTableSet(tableSet);
    }

    return validateTableSet(Collections.<TableInfo>emptySet());

  }

  private Set<TableInfo> validateTableSet(final Set<TableInfo> tableSet) {

    if (tableSet.isEmpty()) {
      Shell.log.warn("No tables found that match your criteria");
    }

    return tableSet;
  }

  /**
   * Wrapper for tablename and id. Comparisions, equals and hash code use tablename (id is ignored)
   */
  private static class TableInfo implements Comparable<TableInfo> {

    private final String name;
    private final String id;

    public TableInfo(final String name, final String id) {
      this.name = name;
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public String getId() {
      return id;
    }

    /**
     * Lexicographically table names
     *
     * @param other
     *          an instance to compare
     * @return 0 if table names are equal, a value less than 0 if less, or a value > 0 if greater
     */
    @Override
    public int compareTo(TableInfo other) {
      return name.compareTo(other.name);
    }

    /**
     * Uses table name for equals to be consistent with compareTo
     *
     * @param o
     *          other
     * @return true if table names match.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      TableInfo tableInfo = (TableInfo) o;
      return name.equals(tableInfo.name);
    }

    /**
     * Uses table name for hashCode to be consistent with equals and compareTo
     *
     * @return hash code of table name
     */
    @Override
    public int hashCode() {
      return Objects.hash(name);
    }
  }

  private List<TabletRowInfo> getTabletRowInfo(TableInfo tableInfo, final Connector connector) {

    if (log.isTraceEnabled()) {
      log.trace("get row info for table name {}" + tableInfo.getName());
    }

    log.debug("scan metadata for tablet info table name: \'{}\', tableId: \'{}\' ",
        tableInfo.getName(), tableInfo.getId());

    List<TabletRowInfo> tResults = getMetadataInfo(connector, tableInfo);

    if (log.isTraceEnabled()) {
      for (TabletRowInfo tabletRowInfo : tResults) {
        log.trace("Tablet info: {}", tabletRowInfo);
      }
    }

    return tResults;
  }

  private Scanner buildMetaScanner(final Connector connector, String id)
      throws TableNotFoundException {

    Scanner scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);

    Range range = MetadataSchema.TabletsSection.getRange(id);
    scanner.setRange(range);

    for (Text cf : TabletRowInfo.COL_FAMILIES) {
      scanner.fetchColumnFamily(cf);
    }

    return scanner;
  }

  private List<TabletRowInfo> getMetadataInfo(Connector connector, TableInfo tableInfo) {
    //
    //
    // // This is created outside of the run loop and passed to the walogCollector so that
    // // only a single timed task is created (internal to LiveTServerSet using SimpleTimer.
    // final LiveTServerSet liveTServerSet = new LiveTServerSet(this, new LiveTServerSet.Listener()
    // {
    // @Override
    // public void update(LiveTServerSet current, Set<TServerInstance> deleted,
    // Set<TServerInstance> added) {
    //
    // log.debug("Number of current servers {}, tservers added {}, removed {}",
    // current == null ? -1 : current.size(), added, deleted);
    //
    // if (log.isTraceEnabled()) {
    // log.trace("Current servers: {}\nAdded: {}\n Removed: {}", current, added, deleted);
    // }
    // }
    // });
    //
    // // now it's safe to get the liveServers
    // liveServers.scanServers();
    // Set<TServerInstance> currentServers = liveServers.getCurrentServers();

    List<TabletRowInfo> results = new ArrayList<>();

    TabletRowInfo.Factory tabletInfoFactory = new TabletRowInfo.Factory(tableInfo.getName());

    try (Scanner scanner = buildMetaScanner(connector, tableInfo.getId())) {

      Text currentRow = new Text("");

      boolean firstRow = true;

      for (Map.Entry<Key,Value> entry : scanner) {

        Text row = entry.getKey().getRow();
        Value value = entry.getValue();

        log.debug("r:{}", row);

        if (row.compareTo(currentRow) != 0) {
          currentRow = row;

          if (firstRow) {
            firstRow = false;
          } else {
            results.add(tabletInfoFactory.build());
          }

          tabletInfoFactory = new TabletRowInfo.Factory(tableInfo.getName());
          tabletInfoFactory.tableId(tableInfo.getId());
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
      TabletRowInfo.Factory factory = new TabletRowInfo.Factory(tableInfo.getName());
      factory.tableId(tableInfo.getId());
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

    noFlushOption =
        new Option("nf", "noFlush", false, "do not flush table data in memory before cloning.");
    opts.addOption(noFlushOption);

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
            return;
          }

          if (cf.compareTo(locCf) == 0) {
            location = value.toString();
            return;
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
