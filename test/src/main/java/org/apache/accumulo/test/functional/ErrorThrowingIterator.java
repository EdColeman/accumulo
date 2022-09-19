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
package org.apache.accumulo.test.functional;

import java.io.IOException;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorThrowingIterator extends WrappingIterator {

  private static final Logger log = LoggerFactory.getLogger(ErrorThrowingIterator.class);

  public ErrorThrowingIterator() {}

  public ErrorThrowingIterator(ErrorThrowingIterator other, IteratorEnvironment env) {
    setSource(other.getSource().deepCopy(env));
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new ErrorThrowingIterator(this, env);
  }

  @Override
  public void next() throws IOException {
    super.next();
    if (hasTop()) {
      String v = getTopValue().toString();
      log.info("CALLED NEXT: {} -> {}", getTopKey(), v);
      Cmd.throwOnMatch(v, "next");
    }
  }

  public static class Cmd {
    private static final String separator = "::";
    private static final String prefix = "throw";

    public static String build(final Exception type, final String methodName, final String msg) {
      StringJoiner sj = new StringJoiner(separator);
      sj.add(prefix);
      sj.add(methodName);
      sj.add(type.getClass().getSimpleName());
      sj.add(msg);
      return sj.toString();
    }

    public static void throwOnMatch(final String str, final String methodName) throws IOException {

      if (str != null && str.startsWith(prefix + separator)) {
        String[] parts = str.split(separator);
        if (parts.length >= 3 && (parts[1].compareTo(methodName) == 0)) {
          String exType = parts[2];
          String exMsg = parts.length > 3 ? parts[3] : "";
          log.info("throwing: {}", exType);

          switch (exType) {
            case "IllegalStateException":
              throw new IllegalStateException(exMsg + ": " + exType);
            case "IOException":
              throw new IOException(exMsg + ": " + exType);
            case "AccumuloException":
              throw new UnsupportedOperationException(
                  "AccumuloException not supported, message " + exMsg);
            default:
              throw new UnsupportedOperationException(
                  "unknown exception type requested: " + exType + " message: " + exMsg);
          }
        }
      }
    }
  }
}
