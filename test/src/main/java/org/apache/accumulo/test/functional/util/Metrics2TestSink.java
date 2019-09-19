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

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Metrics2TestSink implements MetricsSink {

  // OutputStream outStream = null;
  OutputStream outStream;

  @Override public void putMetrics(MetricsRecord metricsRecord) {
    try {
      if (outStream != null) {
        for (AbstractMetric r : metricsRecord.metrics()) {
          String o = String.format("%s, %d\n", r.name(), r.value().longValue());
          outStream.write(o.getBytes());
        }
        outStream.write(String.format("%s\n", metricsRecord.toString()).getBytes());
        flush();
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Override public void flush() {
    try {
      outStream.flush();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  IPCSocket ipc = null;
  Object lock = new Object();

  @Override public void init(SubsetConfiguration subsetConfiguration) {

    synchronized (lock) {
      if (ipc == null) {
        ipc = new IPCSocket(this);
      }
    }

    try {

      String contextValue = subsetConfiguration.getString("context");
      if (contextValue.startsWith(":")) {
        contextValue = contextValue.substring(1, contextValue.length());
      }

      File f = new File(String.format("%s/%s.dat", "/tmp", contextValue));

      outStream = new BufferedOutputStream(new FileOutputStream(f));

    } catch (Exception ex) {
      throw new IllegalStateException("Failed to init test metrics", ex);
    }
  }

  private static class IPCSocket implements Runnable {

    private ServerSocket serverSocket;
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;

    private Metrics2TestSink outer;

    public IPCSocket(Metrics2TestSink outer) {

      this.outer = outer;

      createTestSocket();

      Thread t = new Thread(this);
      t.start();
    }

    private volatile boolean running = true;

    void createTestSocket() {
      try {

        serverSocket = new ServerSocket(12123);
        clientSocket = serverSocket.accept();

        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        out = new PrintWriter(clientSocket.getOutputStream(), true);

      } catch (Exception ex) {

      }
    }

    public void run() {

      while (running) {
        try {
          String greeting = in.readLine();

          outer.outStream.write(greeting.getBytes());

          if ("hello".equals(greeting)) {
            out.println("hello client");
          } else if ("done!".equals(greeting)) {
            out.println("shutdown accepted");
            close();
          } else {
            out.println("what? \'" + greeting +"\'");
          }
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      }
    }

    void close() {
      try {
        in.close();
        out.close();
        clientSocket.close();
        serverSocket.close();
      } catch (IOException ex) {
        // empty
      }
      running = false;
    }
  }
}
