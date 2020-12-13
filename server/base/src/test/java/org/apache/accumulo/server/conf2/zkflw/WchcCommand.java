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
package org.apache.accumulo.server.conf2.zkflw;

import org.apache.accumulo.core.rpc.TTimeoutTransport;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class WchcCommand {

  private static final Logger log = LogManager.getLogger(WchcCommand.class);

  private final static String ZK_WCHC_CMD = "wchc";

  private final static String DEFAULT_FILENAME = "/tmp/cmdOutput.txt";

  private final String zkHost;
  private final int zkPort;
  private final String filename;

  public WchcCommand(final String zkHost, final int zkPort, final String filename) {
    this.zkHost = zkHost;
    this.zkPort = zkPort;
    this.filename = filename;
  }

  public void sendZkWchcCmd() {

    TTransport transport;

    try {

      HostAndPort addr = HostAndPort.fromParts(zkHost, zkPort);

      transport = Objects.requireNonNull(TTimeoutTransport.create(addr, 10 * 1000L),
          "Failed to get socket transport");

      String zkCmdString = ZK_WCHC_CMD + "\n";

      transport.write(zkCmdString.getBytes(UTF_8), 0, zkCmdString.length());

      FileOutputStream fo = new FileOutputStream(filename);

      try (WritableByteChannel out = Channels.newChannel(fo)) {

        transport.flush();

        byte[] buffer = new byte[1024 * 1024];
        ByteBuffer wrapper = ByteBuffer.wrap(buffer);

        int n;
        while ((n = transport.read(buffer, 0, buffer.length)) > 0) {

          wrapper.put(buffer, 0, n);
          wrapper.flip();
          out.write(wrapper);
          wrapper.clear();
        }

      } catch (TTransportException ex) {
        log.debug("Reached end of zookeeper command output");
        // happens at EOF
      }

    } catch (IOException | TTransportException e) {
      e.printStackTrace();
    }
  }

  public List<SplitPath> filterWchcOutput(final Pattern pattern) {

    List<SplitPath> paths = new ArrayList<>();

    try (Stream<String> stream = Files.lines(Paths.get(filename))) {

      stream.map(path -> new SplitPath(path)).filter(line -> regexFilter(pattern, line))
          .forEach(p -> paths.add(p));

    } catch (IOException e) {
      e.printStackTrace();
    }
    return paths;
  }

  private boolean regexFilter(final Pattern pattern, SplitPath path){
    Matcher m = pattern.matcher(path.getZkPath());
    return m.matches();
  }

  public static class SplitPath {

    private final String zkPath;
    private final String[] parts;

    public SplitPath(final String zkPath) {

      if (Objects.isNull(zkPath)) {
        this.zkPath = "";
        this.parts = new String[0];
        return;
      }

      this.zkPath = zkPath.trim();
      parts = zkPath.split("/");
    }

    public String getZkPath() {
      return zkPath;
    }

    public String extractTableId() {
      if (parts.length < 5) {
        log.info("Invalid path - does not seem to have enough / to find table id");
        return "";
      }

      return parts[4];
    }

    @Override public String toString() {
      return "SplitPath{" + "zkPath='" + zkPath + '\'' + '}';
    }
  }
}
