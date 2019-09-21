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

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Metrics2TestSinkTest {
  private static final Logger log = LoggerFactory.getLogger(Metrics2TestSinkTest.class);

  @Test
  public void jsonRoundTrip() {
    Map<String,String> expected = new TreeMap<>();
    expected.put("a", "1");
    expected.put("b", "2");
    Metrics2TestSink.JsonValues serdes = new Metrics2TestSink.JsonValues();

    serdes.setTimestamp(System.currentTimeMillis());
    for (Map.Entry<String,String> e : expected.entrySet()) {
      serdes.addMetric(e.getKey(), e.getValue());
    }
    serdes.sign();
    String json = serdes.toJson();

    log.info("Payload: {}", json);

    Gson gson = new GsonBuilder().create();
    Metrics2TestSink.JsonValues r = Metrics2TestSink.JsonValues.fromJson(json);

    assertEquals(serdes.getTimestamp(), r.getTimestamp());
    assertEquals(serdes.getSignature(), r.getSignature());
    assertEquals(expected, r.getMetrics());
  }
}
