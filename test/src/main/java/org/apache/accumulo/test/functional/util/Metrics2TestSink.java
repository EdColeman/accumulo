package org.apache.accumulo.test.functional.util;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class Metrics2TestSink implements MetricsSink {

  OutputStream outStream = null;

  @Override public void putMetrics(MetricsRecord metricsRecord) {
    try {
      if (outStream != null) {
        for(AbstractMetric r : metricsRecord.metrics()) {
          String o = String.format("%s, %d\n", r.name(), r.value().longValue());
          outStream.write(o.getBytes());
        }
        outStream.write(String.format("%s\n", metricsRecord.toString()).getBytes());
        flush();
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  @Override public void flush() {
    try {
      outStream.flush();
    }catch (IOException ex){
      ex.printStackTrace();
    }
  }

  @Override public void init(SubsetConfiguration subsetConfiguration) {
    try {
      File f = new File("./target/mini-tests/metrics-test.out");
      outStream = new BufferedOutputStream(new FileOutputStream(f));
    } catch (Exception ex) {
      throw new IllegalStateException("Failed to init test metrics", ex);
    }
  }
}
