/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.iterator;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.iteratortest.IteratorTestBase;
import org.apache.accumulo.iteratortest.IteratorTestInput;
import org.apache.accumulo.iteratortest.IteratorTestOutput;
import org.apache.accumulo.iteratortest.IteratorTestParameters;
import org.apache.accumulo.test.functional.ErrorThrowingIterator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorThrowingIteratorTest extends IteratorTestBase {

  private static final Logger log = LoggerFactory.getLogger(ErrorThrowingIterator.class);
  private static final TreeMap<Key,Value> INPUT_DATA = createInputData();
  private static final TreeMap<Key,Value> OUTPUT_DATA = createOutputData();

  @Override
  protected Stream<IteratorTestParameters> parameters() {
    var input =
        new IteratorTestInput(ErrorThrowingIterator.class, Map.of(), new Range(), INPUT_DATA);
    var expectedOutput = new IteratorTestOutput(OUTPUT_DATA);
    return builtinTestCases().map(test -> test.toParameters(input, expectedOutput));
  }

  private static TreeMap<Key,Value> createInputData() {
    TreeMap<Key,Value> data = new TreeMap<>();

    data.put(new Key("1", "", "a"), new Value("1a"));
    data.put(new Key("1", "a", "a"), new Value("1aa"));
    data.put(new Key("1", "b", "a"), new Value("1ba"));
    data.put(new Key("1", "c", "c"), new Value("throw::next()::IllegalStateException::(1cc)"));
    data.put(new Key("2", "", "a"), new Value("2a"));
    data.put(new Key("2", "a", "a"), new Value("2aa"));
    data.put(new Key("2", "b", "a"), new Value("2ba"));
    data.put(new Key("2", "c", "c"), new Value("throw::next()::IOException::(2cc)"));

    return data;
  }

  private static TreeMap<Key,Value> createOutputData() {
    // return new TreeMap<>();
    return createInputData();
  }

  @Test
  public void x() {
    String x = "A::new::string";
    String[] t = x.split("x");
    log.info("t: {}={}", t.length, t);

    t = x.split("::");
    log.info("t: {}={}", t.length, t);
  }

  @Test
  public void cmdBuilder() throws Exception {
    log.info("cmd: {}", ErrorThrowingIterator.Cmd.build(new IllegalStateException(), "next",
        "throw IllegalStateException on next"));
    log.info("cmd: {}", ErrorThrowingIterator.Cmd.build(new IllegalStateException(), "next", ""));

    ErrorThrowingIterator.Cmd.throwOnMatch("a random value", "next");

    String sample1 = ErrorThrowingIterator.Cmd.build(new IllegalStateException(), "next", "");
    assertThrows(IllegalStateException.class,
        () -> ErrorThrowingIterator.Cmd.throwOnMatch(sample1, "next"));

    String sample2 =
        ErrorThrowingIterator.Cmd.build(new IOException(), "aMethod", "expect IOException");
    assertThrows(IOException.class,
        () -> ErrorThrowingIterator.Cmd.throwOnMatch(sample2, "aMethod"));

    String sample3 = ErrorThrowingIterator.Cmd.build(new NullPointerException(), "aMethod",
        "expect UnsupportedOperationException");
    assertThrows(UnsupportedOperationException.class,
        () -> ErrorThrowingIterator.Cmd.throwOnMatch(sample3, "aMethod"));

  }
}
