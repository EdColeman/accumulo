package org.apache.accumulo.test.functional.util;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class Metrics2IPCTest {

  private static final Logger log = LoggerFactory.getLogger(Metrics2IPCTest.class);

  @Test public void connect(){

    try(Metrics2IPC.IpcSocketSink sink = new Metrics2IPC.IpcSocketSink()){

      try(Metrics2IPC.IpcSocketSource source = new Metrics2IPC.IpcSocketSource()){

        Thread.currentThread().sleep(500);

        log.info("send ?");
       sink.send("?".getBytes(StandardCharsets.UTF_8));


        Thread.currentThread().sleep(500);
        log.info("send stop");
        sink.send("stop".getBytes(StandardCharsets.UTF_8));

        Thread.currentThread().sleep(500);

      }catch(Exception ex){
        log.info("Could not create / connect source", ex);
      }

    }catch(Exception ex){
      log.info("Could not create / connect sink", ex);
    }
  }
}
