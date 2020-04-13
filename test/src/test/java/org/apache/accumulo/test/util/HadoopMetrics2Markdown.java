/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopMetrics2Markdown {

  private static final Logger log = LoggerFactory.getLogger(HadoopMetrics2Markdown.class);

  private static final String defaultInFile = "/tmp/it.all.metrics";
  private static String defaultOutFile = "/tmp/metrics.md";

  private final Map<String,Map<TagInfo,Integer>> byMetricName = new TreeMap<>();

  public HadoopMetrics2Markdown(String inFilename, String outFilename) {

    try (Scanner scanner = new Scanner(new File(inFilename))) {

      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        log.trace("Read: {}", line);

        RecordInfo.parse(line).ifPresentOrElse(this::save, () -> log.debug("no match: {}", line));

      }

      log.info("processed records: {}", byMetricName);

      printMarkdown(outFilename, byMetricName);

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  private void printMarkdown(final String filename,
      final Map<String,Map<TagInfo,Integer>> byMetricName) {

    final String mdTableHeader = "| tag name | type |\n| ---- | ---- |";

    try (FileWriter fileWriter = new FileWriter(filename);
        PrintWriter printWriter = new PrintWriter(fileWriter)) {

      byMetricName.forEach((k, v) -> {
        log.debug("record:{} -> tags: {}", k, v);
        printWriter.printf("Record: %s\n", k);
        printWriter.println(mdTableHeader);
        v.forEach((t, tv) -> printWriter.printf("| %s | %s |\n", t.key, t.type.name()));
        printWriter.printf("\n");
      });

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void save(final RecordInfo info) {
    log.trace("save record:{}", info);

    Map<TagInfo,Integer> tagsSeen = byMetricName.computeIfAbsent(info.name, k -> new TreeMap<>());

    info.tags.forEach(t -> {
      if (tagsSeen.computeIfPresent(t, (k, v) -> v + 1) == null) {
        tagsSeen.put(t, 1);
      }
    });
  }

  private static class TagInfo implements Comparable<TagInfo> {

    // private static final Pattern numPattern = Pattern.compile("-?\\d+(\\.\\d+)?");
    private static final Pattern numPattern =
        Pattern.compile("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$");

    @Override
    public int compareTo(TagInfo o) {
      int result = this.type.compareTo(o.type);
      if (result != 0) {
        return result;
      }
      return this.key.compareTo(o.key);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      TagInfo tagInfo = (TagInfo) o;
      return Objects.equals(key, tagInfo.key) && Objects.equals(value, tagInfo.value)
          && type == tagInfo.type;
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value, type);
    }

    enum Type {
      env, number
    }

    final String key;
    final String value;
    final Type type;

    public TagInfo(final String key, final String value) {
      this.key = key;
      type = isNumeric(value) ? Type.number : Type.env;
      this.value = value;
    }

    public boolean isNumeric(String strNum) {
      if (strNum == null) {
        return false;
      }
      return numPattern.matcher(strNum).matches();
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", TagInfo.class.getSimpleName() + "[", "]")
          .add("key='" + key + "'").add("value='" + value + "'").add("type=" + type).toString();
    }
  }

  private static class RecordInfo {

    // pattern match group name constants
    private static final String g_timestamp = "timestamp";
    private static final String g_name = "name";
    private static final String g_tags = "tags";
    // pattern match tag name constants
    private static final String g_tkey = "tkey";
    private static final String g_tvalue = "tvalue";

    // match timestamp, name, [tags...]
    private static final Pattern match_line = Pattern.compile(
        "(?<" + g_timestamp + ">\\d*)\\s*(?<" + g_name + ">\\S*):\\s*(?<" + g_tags + ">.*$)");

    // match tag k,v pairs
    private static final Pattern match_tags =
        Pattern.compile("((?<" + g_tkey + ">\\w*)=(?<" + g_tvalue + ">.+?(?=(?=,\\s\\w)|$)))+");

    final String timestamp;
    final String name;
    final List<TagInfo> tags = new ArrayList<>();

    private RecordInfo(final String timestamp, final String name) {
      this.timestamp = timestamp;
      this.name = name;
    }

    void addTag(TagInfo tag) {
      tags.add(tag);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", RecordInfo.class.getSimpleName() + "[", "]")
          .add("timestamp='" + timestamp + "'").add("name='" + name + "'").add("tags=" + tags)
          .toString();
    }

    static Optional<RecordInfo> parse(final String line) {

      // the : should appear once, if more, it partial records may have been written
      long count = line.chars().filter(ch -> ch == ':').count();
      if (count != 1) {
        log.debug("Possible corrupt record, multiple :. Received '{}'", line);
        return Optional.empty();
      }

      Matcher lineMatcher = match_line.matcher(line);
      if (!lineMatcher.matches()) {
        return Optional.empty();
      }

      var timestamp = lineMatcher.group(g_timestamp);
      var name = lineMatcher.group(g_name);

      RecordInfo record = new RecordInfo(timestamp, name);

      Matcher m = match_tags.matcher(lineMatcher.group(g_tags));

      while (m.find()) {
        TagInfo tag = new TagInfo(m.group(g_tkey), m.group(g_tvalue));
        log.trace("TAG:{}", tag);
        record.addTag(tag);
      }

      return Optional.of(record);

    }
  }

  public static void main(String... args) {

    if (args.length == 0) {
      new HadoopMetrics2Markdown(defaultInFile, defaultOutFile);
    }
  }
}
