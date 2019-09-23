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
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Metrics2SocketIpc {

  private final static int port = 12332;

  public static class IpcSocketSink implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(IpcSocketSink.class);

    private Metrics2ProtocolHandler handler;

    private ServerSocket serverSocket;
    private Socket client;

    private volatile boolean running = true;

    public IpcSocketSink() {

      try {
        serverSocket = new ServerSocket(port, 0, InetAddress.getLoopbackAddress());
      } catch (IOException e) {
        throw new IllegalStateException("Failed to create socket connection for sink.");
      }

      Thread t = new Thread(this);
      t.start();
    }

    @Override
    public void run() {
      try {
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

    public String read() {
      if (client.isConnected()) {
        return handler.read();
      }
      return "";
    }

    @Override
    public synchronized void close() throws IOException {
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

    private volatile boolean running = true;

    public IpcSocketSource() {
      boolean connected = false;

      int failureCount = 0;

      final long[] backoff = {500, 500, 1_000, 3_000, 5_000};

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

      public String read(){
        if (socket.isConnected()) {
          return handler.read();
        }
        return "";
    }

    @Override
    public synchronized void close() throws IOException {
      if (socket != null) {
        Socket closing = socket;
        socket = null;
        closing.close();
      }
    }

    @Override
    public void run() {

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
