package org.apache.accumulo.test.functional.util;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicBoolean;

public class SinkStub implements MetricsSink {

  private final AtomicBoolean initialized = new AtomicBoolean(Boolean.FALSE);

  PrintWriter pw = null;

  @Override public void putMetrics(MetricsRecord metricsRecord) {

    for (AbstractMetric r : metricsRecord.metrics()) {
      pw.println(String.format("%s=%s", r.name(), Long.toString(r.value().longValue())));
    }

    pw.flush();
  }

  @Override public void flush() {
    if (pw != null) {
      pw.flush();
    }
  }

  @Override public void init(SubsetConfiguration subsetConfiguration) {

    synchronized (initialized) {

      if (initialized.get()) {
        return;
      }

      try {
        FileWriter fw = new FileWriter("/tmp/test-sink.txt"); //this erases previous content
        fw = new FileWriter("/tmp/test-sink.txt", true); //this reopens file for appending
        BufferedWriter bw = new BufferedWriter(fw);
        pw = new PrintWriter(bw);
      } catch (IOException ex) {
        ex.printStackTrace();
        return;
      }

      initialized.set(Boolean.TRUE);

    }
  }
}
