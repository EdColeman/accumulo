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

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellOptions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

import java.math.BigInteger;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

/**
 * Copied from ACCUMULO-2873
 * It would be very useful to have a utility that generates single line tablet info.  The output of this could be fed
 * to sort, awk, grep, etc inorder to answer questions like which tablets have the most files.The output could look
 * something like the following
 * <pre>
 * $accumulo admin listTablets --table bigTable3
 * #files #walogs #entries #size #status #location #tableid #endrow
 * 6 2 40,001 50M ASSIGNED 10.1.9.9 4:9997[abc]  3 admin
 * 3 1 50,002 40M ASSIGNED 10.1.9.9 5:9997[abc]  3 helpful
 * </pre>
 * All of the information can be obtained by scanning the metadata table and looking into zookeeper.
 * Could possibly contact tablet servers to get info about entires in memory.
 * The order of the columns in the example above is arbitrary, except for end row.  Maybe end row column should
 * come last because it can be of arbitrary length.  Also the end row could contain any character,
 * could look into using a CSV library.   It would be nice to design the utility so
 * that columns can be added in future versions w/o impacting current scripts that use the utility.
 */
public class ListTabletsCommand extends Command {

    private Option outputFileOpt;
    private Option optTablePattern;
    private Option optHumanReadble;
    private Option optNamespace;

    @Override
    public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {

        // Arrays.asList(cl.getArgs()
        final SortedSet<String> tables = new TreeSet<>();

        String o = cl.getOptionValue(ShellOptions.tableOption);

        if (cl.hasOption(ShellOptions.tableOption)) {
            tables.add(cl.getOptionValue(ShellOptions.tableOption));
        }

        if (cl.hasOption(optNamespace.getOpt())) {
            Instance instance = shellState.getInstance();
            String namespaceId =
                    Namespaces.getNamespaceId(instance, cl.getOptionValue(optNamespace.getOpt()));
            tables.addAll(Namespaces.getTableNames(instance, namespaceId));
        }

        boolean prettyPrint = cl.hasOption(optHumanReadble.getOpt()) ? true : false;

        // Add any patterns
        if (cl.hasOption(optTablePattern.getOpt())) {
            for (String table : shellState.getConnector().tableOperations().list()) {
                if (table.matches(cl.getOptionValue(optTablePattern.getOpt()))) {
                    tables.add(table);
                }
            }
        }

        // If we didn't get any tables, and we have a table selected, add the current table
        if (tables.isEmpty() && !shellState.getTableName().isEmpty()) {
            tables.add(shellState.getTableName());
        }
//
//        // sanity check...make sure the user-specified tables exist
//        for (String tableName : tables) {
//            if (!shellState.getConnector().tableOperations().exists(tableName)) {
//                throw new TableNotFoundException(tableName, tableName,
//                        "specified table that doesn't exist");
//            }
//        }

        try {

            // String valueFormat = prettyPrint ? "%9s" : "%,24d";

//            List<String> lines = new ArrayList<>();
//
//            lines.add("NO TABLE ????");

            for (TabletInfo tabletInfo : getTabletInfo(tables, shellState.getConnector())) {
                shellState.getReader()
                        .println(String.format(tabletInfo.toString()));
            }

//            shellState.printLines(lines.iterator(), false);

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return 0;
    }

    private List<TabletInfo> getTabletInfo(SortedSet<String> tables, final Connector connector) {

        System.out.println("Looks:");

        for (String e : tables) {
            System.out.println("L: " + e);
        }

        Map<String, String> nameIdMap = connector.tableOperations().tableIdMap();

        for (Map.Entry<String, String> e : nameIdMap.entrySet()) {
            System.out.println("N: " + e.getKey() + ": " + e.getValue());
        }

        Authorizations auth = new Authorizations();
        List<TabletInfo> tablets = new ArrayList(tables.size());

        for (String tableName : tables) {

            System.out.println("LT: " + tableName);

            String id = nameIdMap.get(tableName);

            System.out.println("LT ID: " + id);

            if (id == null) {
                TabletInfo.Builder builder = new TabletInfo.Builder(tableName);
                tablets.add(builder.build());
                continue;
            }

            try (Scanner scanner = connector.createScanner("accumulo.metadata", auth)) {

                System.out.println(String.format("SCANNER: %s - %s - R: %s", tableName, id, MetadataSchema.TabletsSection.getRange(id).toString()));

                scanner.setRange(MetadataSchema.TabletsSection.getRange(id));

                for (Text cf : TabletInfo.COL_FAMS) {
                    scanner.fetchColumnFamily(cf);
                }

                Text currentRow = new Text("");
                TabletInfo.Builder builder = null;

                for (Map.Entry<Key, Value> entry : scanner) {

                    Text row = entry.getKey().getRow();
                    Value value = entry.getValue();

                    if(row.compareTo(currentRow) != 0){
                        currentRow = row;

                        if(builder != null) {
                            tablets.add(builder.build());
                        }

                        builder = new TabletInfo.Builder(tableName);
                        builder.tableId(id);
                        builder.tableExists(true);
                        builder.updateInfo(entry.getKey(), value);

                    } else {
                        builder.updateInfo(entry.getKey(), value);
                    }
                }
                // emit last row
                if(builder != null){
                    tablets.add(builder.build());
                }

            } catch (TableNotFoundException ex) {
                ex.printStackTrace();
                TabletInfo.Builder builder = new TabletInfo.Builder(tableName);
                builder.tableId(id);
                builder.tableExists(false);
                tablets.add(builder.build());
                continue;
            }

        }

        System.out.println("founds:");
        for (TabletInfo e : tablets) {
            System.out.println("F: " + e);
        }

        return tablets;
    }


    static class TabletInfo {

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

        private final BigInteger tag = new BigInteger(32, ThreadLocalRandom.current());

        private TabletInfo(String tableName, int numFiles, int numWalLogs, long numEntries, long size, String status, String location, String tableId, String endRow, boolean tableExists) {
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

        public String getTableName() {
            return tableName;
        }

        public int getNumFiles() {
            return numFiles;
        }

        public int getNumWalLogs() {
            return numWalLogs;
        }

        public long getNumEntries() {
            return numEntries;
        }

        public long getSize() {
            return size;
        }

        public String getStatus() {
            return status;
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

        static Text fileCf = MetadataSchema.TabletsSection.DataFileColumnFamily.NAME;
        static Text locCf = MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME;
        static Text logCf = MetadataSchema.TabletsSection.LogColumnFamily.NAME;
        static Text tabCf = MetadataSchema.TabletsSection.TabletColumnFamily.NAME;

        static Text[] COL_FAMS = {fileCf, locCf, logCf, tabCf};

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

        public static class Builder {
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
            List<Throwable> exceptions = new ArrayList<>();

            public Builder(final String tableName) {
                this.tableName = tableName;
            }

            public Builder numFiles(int numFiles) {
                this.numFiles = numFiles;
                return this;
            }

            public Builder numWalLogs(int numWalLogs) {
                this.numWalLogs = numWalLogs;
                return this;
            }

            public Builder numEntries(long numEntries) {
                this.numEntries = numEntries;
                return this;
            }

            public Builder size(int size) {
                this.size = size;
                return this;
            }

            public Builder status(String status) {
                this.status = status;
                return this;
            }

            public Builder location(String location) {
                this.location = location;
                return this;
            }

            public Builder tableId(String tableId) {
                this.tableId = tableId;
                return this;
            }

            public Builder endRow(String endRow) {
                this.endRow = endRow;
                return this;
            }

            public Builder tableExists(boolean tableExists) {
                this.tableExists = tableExists;
                return this;
            }

            public Builder updateInfo(final Key key, final Value value) {
                Text cf = key.getColumnFamily();

                try {
                    if (cf.compareTo(fileCf) == 0) {
                        numFiles += 1;
                        String[] tokens = value.toString().split(",");
                        if (tokens.length == 2) {
                            size += Long.parseLong(tokens[0]);
                            numEntries += Long.parseLong(tokens[1]);
                        }
                        return this;
                    }

                    if (cf.compareTo(locCf) == 0) {
                        location = value.toString();
                        return this;
                    }

                    if (cf.compareTo(logCf) == 0) {
                        numWalLogs++;
                        return this;
                    }

                }catch(NumberFormatException ex){
                    parseErrors++;
                    exceptions.add(ex);
                }

                return this;
            }

            public TabletInfo build() {
                return new TabletInfo(tableName, numFiles, numWalLogs, numEntries, size, status, location, tableId, endRow, tableExists);
            }
        }
    }

    @Override
    public String description() {
        return null;
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

        optHumanReadble =
                new Option("h", "human-readable", false, "format large sizes to human readable units");
        optHumanReadble.setArgName("human readable output");
        opts.addOption(optHumanReadble);

        optNamespace =
                new Option(ShellOptions.namespaceOption, "namespace", true, "name of a namespace");
        optNamespace.setArgName("namespace");
        opts.addOption(optNamespace);

        outputFileOpt = new Option("o", "output", true, "local file to write output to");
        outputFileOpt.setArgName("file");
        opts.addOption(outputFileOpt);

        return opts;
    }
}
