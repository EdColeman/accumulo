package org.apache.accumulo.shell.commands;

import jline.console.ConsoleReader;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.ShellOptions;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.*;

public class ListTabletsTest {

    @Test public void mockTest() throws Exception {

        ListTablets cmd = new ListTablets();

        Connector conn = EasyMock.createMock(Connector.class);
        TableOperations tableOps = EasyMock.createMock(TableOperations.class);
        Shell shellState = EasyMock.createMock(Shell.class);
        ConsoleReader reader = EasyMock.createMock(ConsoleReader.class);

        Options opts = cmd.getOptions();

        CommandLineParser parser = new BasicParser();
        String[] args = {"-t", "aTable"};
        CommandLine cli = parser.parse(opts,args);

        EasyMock.expect(shellState.getConnector()).andReturn(conn);
        EasyMock.expect(conn.tableOperations()).andReturn(tableOps);

        Map<String,String> idMap = new TreeMap<>();
        idMap.put("aTable", "123");

        EasyMock.expect(tableOps.tableIdMap()).andReturn(idMap);

        //EasyMock.expect(cli.hasOption("t")).andReturn(true);
        //EasyMock.expect(cli.hasOption("t")).andReturn(true);


        EasyMock.replay(conn, tableOps, shellState, reader);

        cmd.execute("listTablets -t aTable", cli, shellState);

        EasyMock.verify(conn, tableOps, shellState, reader);
    }
    @Test
    public void defaultBuilderTest(){

        ListTablets.TabletInfo.Builder builder = new ListTablets.TabletInfo.Builder("aName");

        ListTablets.TabletInfo info = builder.build();

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
    public void builderTest(){

        ListTablets.TabletInfo.Builder builder = new ListTablets.TabletInfo.Builder("aName")
                .numFiles(1).numWalLogs(2).numEntries(3).size(4).status("status").location("loc")
                .tableId("123").endRow("end").tableExists(true);

        ListTablets.TabletInfo info = builder.build();

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
