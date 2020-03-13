package org.apache.accumulo.test.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HadoopMetrics2Markdown {

  private static final Logger log = LoggerFactory.getLogger(HadoopMetrics2Markdown.class);

  private static final String defaultInFile = "/tmp/it.all.metrics";
  private static String defaultOutFile = "/tmp/metrics.md";

  public HadoopMetrics2Markdown(String filename) {

    try (Scanner scanner = new Scanner(new File(filename))) {

      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        log.debug("Read: {}", line);
        log.debug("P: {}", RecordInfo.parse(line));
      }

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  private static class TagPair {
    final String key;
    final String value;

    public TagPair(final String key, final String value) {
      this.key = key;
      this.value = value;
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    @Override public String toString() {
      return new StringJoiner(", ", TagPair.class.getSimpleName() + "[", "]")
          .add("key='" + key + "'").add("value='" + value + "'").toString();
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
        "(?<" + g_timestamp + ">\\w*)\\s*(?<" + g_name + ">\\S*):\\s*(?<" + g_tags + ">.*$)");

    // match tag k,v pairs
    private static final Pattern match_tags = Pattern
        .compile("((?<" + g_tkey + ">\\w*)=(?<" + g_tvalue + ">.+?(?=(?=,\\s\\w)|$)))+");

    final String timestamp;
    final String name;
    final List<TagPair> tags = new ArrayList<>();

    private RecordInfo(final String timestamp, final String name) {
      this.timestamp = timestamp;
      this.name = name;
    }

    void addTag(TagPair tag) {
      tags.add(tag);
    }

    @Override public String toString() {
      return new StringJoiner(", ", RecordInfo.class.getSimpleName() + "[", "]")
          .add("timestamp='" + timestamp + "'").add("name='" + name + "'").add("tags=" + tags)
          .toString();
    }

    static RecordInfo parse(final String line) {

      Matcher lineMatcher = match_line.matcher(line);
      if (!lineMatcher.matches()) {
        return null;
      }

      var timestamp = lineMatcher.group(g_timestamp);
      var name = lineMatcher.group(g_name);

      RecordInfo record = new RecordInfo(timestamp, name);

      Matcher m = match_tags.matcher(lineMatcher.group(g_tags));

      while (m.find()) {
        TagPair tag = new TagPair(m.group(g_tkey), m.group(g_tvalue));
        log.trace("TAG:{}", tag);
        record.addTag(tag);
      }

      return record;

    }
  }

  public static void main(String... args) {

    if (args.length == 0) {
      new HadoopMetrics2Markdown(defaultInFile);
    }
  }
}
