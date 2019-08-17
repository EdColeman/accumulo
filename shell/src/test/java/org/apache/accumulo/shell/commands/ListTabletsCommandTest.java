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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jline.console.ConsoleReader;

public class ListTabletsCommandTest {

  private static final Logger log = LoggerFactory.getLogger(ListTabletsCommandTest.class);

  @Test
  public void mockTest() throws Exception {

    ListTabletsCommand cmd = new ListTabletsCommand();

    Connector conn = EasyMock.createMock(Connector.class);
    Scanner scanner = EasyMock.createMock(Scanner.class);

    TableOperations tableOps = EasyMock.createMock(TableOperations.class);
    Shell shellState = EasyMock.createMock(Shell.class);
    ConsoleReader reader = EasyMock.createMock(ConsoleReader.class);

    Options opts = cmd.getOptions();

    CommandLineParser parser = new BasicParser();
    String[] args = {"-t", "aTable"};
    CommandLine cli = parser.parse(opts, args);

    EasyMock.expect(shellState.getConnector()).andReturn(conn);
    EasyMock.expect(conn.createScanner("aTable", new Authorizations())).andReturn(scanner);
    EasyMock.expect(conn.tableOperations()).andReturn(tableOps);

    Map<String,String> idMap = new TreeMap<>();
    idMap.put("aTable", "123");

    EasyMock.expect(tableOps.tableIdMap()).andReturn(idMap);

    Map<Key,Value> meta = new TreeMap<>();
    meta.put(new Key("123;a", "file", "hdfs://a/b/c/a.rf"), new Value("111,123"));
    meta.put(new Key("123;a", "loc", "srv1"), new Value("srv1"));
    meta.put(new Key("123;a", "log", "hdfs://a/b/wal.rf"), new Value("hdfs://a/b/wal.rf"));
    meta.put(new Key("123;a", "file", "hdfs://a/b/c/a2.rf"), new Value("888,123"));
    meta.put(new Key("123;b", "file", "hdfs://a/b/c/b.rf"), new Value("222,888"));
    meta.put(new Key("123;b", "loc", "srv2"), new Value("srv1"));
    meta.put(new Key("123<", "file", "hdfs://a/b/c/d.rf"), new Value("333,777"));
    meta.put(new Key("123<", "loc", "srv1"), new Value("srv1"));

    EasyMock.expect(scanner.iterator()).andReturn(meta.entrySet().iterator());
    scanner.setRange(MetadataSchema.TabletsSection.getRange("123"));
    EasyMock.expectLastCall();
    for (Text cf : ListTabletsCommand.TabletRowInfo.COL_FAMILIES) {
      scanner.fetchColumnFamily(cf);
      EasyMock.expectLastCall();
    }

    scanner.close();
    EasyMock.expectLastCall();

    // EasyMock.expect(cli.hasOption("t")).andReturn(true);
    // EasyMock.expect(cli.hasOption("t")).andReturn(true);

    EasyMock.replay(conn, scanner, tableOps, shellState, reader);

    cmd.execute("listTablets -t aTable", cli, shellState);

    EasyMock.verify(conn, tableOps, shellState, reader);
  }

  @Test
  public void defaultBuilderTest() {

    ListTabletsCommand.TabletRowInfo.Factory factory =
        new ListTabletsCommand.TabletRowInfo.Factory("aName");

    ListTabletsCommand.TabletRowInfo info = factory.build();

    assertEquals("aName", info.getTableName());
    assertEquals(0, info.getNumFiles());
    assertEquals(0, info.getNumWalLogs());
    assertEquals(0, info.getNumEntries());
    assertEquals(0, info.getSize());
    assertEquals("UNK", info.getStatus());
    assertEquals("", info.getLocation());
    assertEquals("", info.getTableId());
    assertEquals("", info.getEndRow());
    assertFalse(info.tableExists());

  }

  @Test
  public void builderTest() {

    ListTabletsCommand.TabletRowInfo.Factory factory =
        new ListTabletsCommand.TabletRowInfo.Factory("aName").numFiles(1).numWalLogs(2)
            .numEntries(3).size(4).status("status").location("loc").tableId("123").endRow("end")
            .tableExists(true);

    ListTabletsCommand.TabletRowInfo info = factory.build();

    assertEquals("aName", info.getTableName());
    assertEquals(1, info.getNumFiles());
    assertEquals(2, info.getNumWalLogs());
    assertEquals(3, info.getNumEntries());
    assertEquals(4, info.getSize());
    assertEquals("RECOVERING", info.getStatus());
    assertEquals("loc", info.getLocation());
    assertEquals("123", info.getTableId());
    assertEquals("end", info.getEndRow());
    assertTrue(info.tableExists());

  }

  @Test
  public void foo() throws Exception {

    Connector conn = EasyMock.createMock(Connector.class);
    Scanner scanner = EasyMock.createMock(Scanner.class);

    EasyMock.expect(conn.createScanner("aTable", new Authorizations())).andReturn(scanner);

    Map<Key,Value> meta = new TreeMap<>();
    Key k1 = new Key(new Text("a"));
    Value v1 = new Value("v-1");

    meta.put(k1, v1);
    //
    // Iterator<Map.Entry<Key,Value>> itor =
    // EasyMock.createMock(Iterator.class);

    EasyMock.expect(scanner.iterator()).andReturn(meta.entrySet().iterator());

    // EasyMock.expect(scanner.iterator().hasNext()).andReturn(true);
    // EasyMock.expect(scanner.iterator().next()).andReturn(meta.entrySet().iterator().next());

    EasyMock.replay(conn, scanner);

    Iterator<Map.Entry<Key,Value>> itor = scanner.iterator();

    Map.Entry<Key,Value> x = itor.next();

    log.info("x {}", x);
  }
}
