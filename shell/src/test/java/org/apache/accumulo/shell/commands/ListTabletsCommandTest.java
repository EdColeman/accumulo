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

import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.easymock.EasyMock;
import org.junit.Test;

import jline.console.ConsoleReader;

public class ListTabletsCommandTest {

  @Test
  public void mockTest() throws Exception {

    ListTabletsCommand cmd = new ListTabletsCommand();

    Connector conn = EasyMock.createMock(Connector.class);
    TableOperations tableOps = EasyMock.createMock(TableOperations.class);
    Shell shellState = EasyMock.createMock(Shell.class);
    ConsoleReader reader = EasyMock.createMock(ConsoleReader.class);

    Options opts = cmd.getOptions();

    CommandLineParser parser = new BasicParser();
    String[] args = {"-t", "aTable"};
    CommandLine cli = parser.parse(opts, args);

    EasyMock.expect(shellState.getConnector()).andReturn(conn);
    EasyMock.expect(conn.tableOperations()).andReturn(tableOps);

    Map<String,String> idMap = new TreeMap<>();
    idMap.put("aTable", "123");

    EasyMock.expect(tableOps.tableIdMap()).andReturn(idMap);

    // EasyMock.expect(cli.hasOption("t")).andReturn(true);
    // EasyMock.expect(cli.hasOption("t")).andReturn(true);

    EasyMock.replay(conn, tableOps, shellState, reader);

    cmd.execute("listTablets -t aTable", cli, shellState);

    EasyMock.verify(conn, tableOps, shellState, reader);
  }

  @Test
  public void defaultBuilderTest() {

    ListTabletsCommand.TabletPrintInfo.Factory factory =
        new ListTabletsCommand.TabletPrintInfo.Factory("aName");

    ListTabletsCommand.TabletPrintInfo info = factory.build();

    assertEquals("aName", info.getTableName());
    assertEquals(0, info.getNumFiles());
    assertEquals(0, info.getNumWalLogs());
    assertEquals(0, info.getNumEntries());
    assertEquals(0, info.getSize());
    assertEquals("", info.getStatus());
    assertEquals("", info.getLocation());
    assertEquals("", info.getTableId());
    assertEquals("", info.getEndRow());
    assertFalse(info.tableExists());

  }

  @Test
  public void builderTest() {

    ListTabletsCommand.TabletPrintInfo.Factory factory =
        new ListTabletsCommand.TabletPrintInfo.Factory("aName").numFiles(1).numWalLogs(2)
            .numEntries(3).size(4).status("status").location("loc").tableId("123").endRow("end")
            .tableExists(true);

    ListTabletsCommand.TabletPrintInfo info = factory.build();

    assertEquals("aName", info.getTableName());
    assertEquals(1, info.getNumFiles());
    assertEquals(2, info.getNumWalLogs());
    assertEquals(3, info.getNumEntries());
    assertEquals(4, info.getSize());
    assertEquals("status", info.getStatus());
    assertEquals("loc", info.getLocation());
    assertEquals("123", info.getTableId());
    assertEquals("end", info.getEndRow());
    assertTrue(info.tableExists());

  }

}
