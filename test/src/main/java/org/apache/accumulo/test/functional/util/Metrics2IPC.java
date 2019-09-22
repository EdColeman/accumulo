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

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metrics2IPC {

  private static final String rootDir = "/tmp";
  private static final String extension = "ipc";

  private static final int BUFFER_SIZE = 32 * 1024;

  private static class IpcFile implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(IpcSink.class);

    enum Mode {
      SINK("rw"), SOURCE("r");
      private final String mode;

      Mode(String mode) {
        this.mode = mode;
      }
    }

    private RandomAccessFile aFile;
    private FileChannel channel = null;
    private final ByteBuffer buffer;
    private final Mode mode;

    private IpcFile(final String componentName, final Mode mode) {
      this.mode = mode;
      try {
        aFile = new RandomAccessFile(String.format("%s/%s.%s", rootDir, componentName, extension),
            mode.mode);
        channel = aFile.getChannel();
        if (mode.equals(Mode.SINK)) {
          channel.truncate(0);
        }
      } catch (IOException ex) {
        aFile = null;
        log.info("Failed to create metrics test file ipc channel", ex);
      }

      buffer = ByteBuffer.allocate(BUFFER_SIZE);
    }

    @Override public synchronized void close() {
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

      buffer.clear();
      buffer.put(data);

      buffer.flip();
      try {
        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }
      } catch (IOException ex) {
        log.debug("metrics ipc write failed", ex);
        return false;
      }
      return true;
    }

    byte[] read() {
      if (channel == null) {
        return new byte[0];
      }

      buffer.clear();

      try {

        boolean haveStart = false;
        boolean haveEnd = false;

        while (channel.read(buffer) != -1)
          ;

        int pos = buffer.position();

        buffer.flip();
        log.trace("Read {}", new String(buffer.array(), 0, pos, StandardCharsets.UTF_8));

        byte[] data = new byte[pos];

        buffer.get(data, 0, pos);

        return data;

      } catch (IOException ex) {
        log.debug("metrics ipc write failed", ex);
        return new byte[0];
      }
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

    long currPos() {
      try {
        log.trace("POS: {}", channel.size());
        return channel.size();
      } catch (IOException ex) {
        return -1;
      }
    }
  }

  static class IpcSink extends IpcFile {

    public IpcSink(final String componentName) {
      super(componentName, Mode.SINK);
    }

  }

  public static class FileSource extends IpcFile {

    private static final Logger log = LoggerFactory.getLogger(FileSource.class);

    private long filePos = -1;

    public FileSource(final String componentName) {
      super(componentName, Mode.SOURCE);
    }

    public byte[] blocking() {
      if (filePos < currPos()) {
        byte[] data = read();
        filePos = currPos();
        return data;
      }
      return null;
    }
  }

  final static int port = 12332;

  public static class IpcSocketSink implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(IpcSocketSink.class);

    private Metrics2ProtocolHandler handler;

    private ServerSocket serverSocket;
    private Socket client;

    private volatile boolean running = true;

    public IpcSocketSink() {
      Thread t = new Thread(this);
      t.start();
    }

    @Override public void run() {
      try {
        serverSocket = new ServerSocket(port, 0, InetAddress.getLoopbackAddress());

        // serverSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));

        boolean connected = false;

        while (running && !connected) {
          client = serverSocket.accept();
          connected = client.isConnected();
          log.info("ACCEPTED connection");
        }

        DataInputStream in = new DataInputStream(new BufferedInputStream(client.getInputStream()));
        DataOutputStream out =
            new DataOutputStream(new BufferedOutputStream(client.getOutputStream()));

        handler = new Metrics2ProtocolHandler(in, out);

        while (running) {
          in.readInt();
        }
      } catch (IOException ex) {
        log.info("Failed to open server socket", ex);
      }
    }

    public void send(final byte[] payload) {
      if (client.isConnected()) {
        handler.send(payload);
      }
    }

    @Override public synchronized void close() throws Exception {
      if (serverSocket != null) {
        running = false;
        ServerSocket closing = serverSocket;
        serverSocket = null;
        closing.close();
      }
    }

  }

  public static class IpcSocketSource implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(IpcSocketSource.class);
    private Socket socket;

    private Metrics2ProtocolHandler handler;

    private long[] backoff = {500, 500, 1_000, 3_000, 5_000};

    private volatile boolean running = true;

    public IpcSocketSource() {
      boolean connected = false;

      int failureCount = 0;

      while (!connected && failureCount < backoff.length) {
        try {

          socket = new Socket(InetAddress.getLoopbackAddress(), port);

          DataInputStream in =
              new DataInputStream(new BufferedInputStream(socket.getInputStream()));
          DataOutputStream out =
              new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

          handler = new Metrics2ProtocolHandler(in, out);

          connected = true;

        } catch (IOException ex) {
          try {
            log.debug("failure count {}", failureCount);
            Thread.sleep(backoff[failureCount++]);
          } catch (InterruptedException iex) {
            Thread.currentThread().interrupt();
          }
          log.trace("Failed to open client socket retries=" + failureCount, ex);
        }
      }

      if (!connected) {
        throw new IllegalStateException("Source failed to collect as client: " + failureCount);
      }

      Thread t = new Thread(this);
      t.start();
    }

    public void send(final byte[] payload) {
      if (socket.isConnected()) {
        handler.send(payload);
      }
    }

    @Override public synchronized void close() throws IOException {
      if (socket != null) {
        Socket closing = socket;
        socket = null;
        closing.close();
      }
    }

    @Override public void run() {

      ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

      String stop = "stop";

      try {

        while (running) {
          log.info("start blocking read");
          String r = handler.read();

          log.info("Received {}", r);

          if (stop.equals(r)) {
            log.info("Source received stop - closing connection");
            Thread.sleep(1_000);
            running = false;
            close();
          }

        }
      } catch (Exception ex) {
        log.debug("Source read failed", ex);
      }
    }
  }
}
