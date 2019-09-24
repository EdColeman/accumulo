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
package org.apache.accumulo.test.functional.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IpcFile implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(IpcFile.class);

  private static final String rootDir = "/tmp";
  private static final String extension = "ipc";

  enum Mode {
    SINK("rw"), SOURCE("r");
    private final String mode;

    Mode(String mode) {
      this.mode = mode;
    }
  }

  private Metrics2ProtocolHandler handler;

  private RandomAccessFile aFile;
  private FileChannel channel = null;
  // private final ByteBuffer buffer;
  private final Mode mode;

  private IpcFile(final String componentName, final Mode mode) {

    this.mode = mode;

    final String filename = String.format("%s/%s.%s", rootDir, componentName, extension);

    try {

      aFile = new RandomAccessFile(filename, mode.mode);
      channel = aFile.getChannel();
      if (mode.equals(Mode.SINK)) {
        channel.truncate(0);
      }
    } catch (IOException ex) {
      aFile = null;
      log.info("Failed to create metrics test file ipc channel", ex);
    }

    if (channel == null) {
      throw new IllegalStateException("Could not create file channel " + filename);
    }

    DataInputStream in =
        new DataInputStream(new BufferedInputStream(Channels.newInputStream(channel)));
    DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(Channels.newOutputStream(channel)));

    handler = new Metrics2ProtocolHandler(in, out);

  }

  @Override
  public synchronized void close() {
    try {
      if (mode.equals(Mode.SINK)) {
        channel.force(true);
      }
      channel.close();
      aFile.close();

      channel = null;
      aFile = null;
    } catch (IOException ex) {
      // empty
    }
  }

  public boolean append(final byte[] data) {
    if (channel == null) {
      return false;
    }
    handler.send(data);
    return true;
  }

  public String read() {
    if (channel == null) {
      return "";
    }
    return handler.read();
  }

  public synchronized void flush() {
    try {
      if (channel != null) {
        channel.force(true);
      }
    } catch (IOException ex) {
      log.debug("metrics test ipc flush failed", ex);
    }
  }

  static class IpcSink extends IpcFile {

    public IpcSink(final String componentName) {
      super(componentName, Mode.SINK);
    }

  }

  public static class FileSource extends IpcFile {

    private static final Logger log = LoggerFactory.getLogger(FileSource.class);

    public FileSource(final String componentName) {
      super(componentName, Mode.SOURCE);
    }

  }

}
