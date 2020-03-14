package org.apache.accumulo.test.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

        RecordInfo.parse(line)
            .ifPresentOrElse(this::save, () -> log.debug("no match: {}", line));

      }


      log.info("MM: {}", byMetricName);

      printMarkdown(outFilename, byMetricName);

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  private void printMarkdown(final String filename,
      final Map<String,Map<TagInfo,Integer>> byMetricName) {

    try (FileWriter fileWriter = new FileWriter(filename);
        PrintWriter printWriter = new PrintWriter(fileWriter)) {

      byMetricName.forEach((k, v) -> {
        log.debug("record:{} -> tags: {}", k, v);
        printWriter.printf("Record: %s\n", k);
        printWriter.println("| tag name | type |");
        v.forEach((t, tv) -> printWriter.printf("| %s | %s |\n", t.key, t.type.name()));
        printWriter.printf("\n");
      });

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void save(final RecordInfo info) {
    log.trace("save record:{}", info);

    Map<TagInfo,Integer> tagsSeen = byMetricName
        .computeIfAbsent(info.name, k -> new TreeMap<>());

    info.tags.forEach(t -> {
      if (tagsSeen.computeIfPresent(t, (k, v) -> v + 1) == null) {
        tagsSeen.put(t, 1);
      }
    });
  }

  private static class TagInfo implements Comparable<TagInfo>{

    // private static final Pattern numPattern = Pattern.compile("-?\\d+(\\.\\d+)?");
    private static final Pattern numPattern =
        Pattern.compile("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$");

    @Override public int compareTo(TagInfo o) {
      int result = this.type.compareTo(o.type);
      if(result != 0){
        return result;
      }
      return this.key.compareTo(o.key);
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

    @Override public String toString() {
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
    private static final Pattern match_tags = Pattern
        .compile("((?<" + g_tkey + ">\\w*)=(?<" + g_tvalue + ">.+?(?=(?=,\\s\\w)|$)))+");

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

    @Override public String toString() {
      return new StringJoiner(", ", RecordInfo.class.getSimpleName() + "[", "]")
          .add("timestamp='" + timestamp + "'").add("name='" + name + "'").add("tags=" + tags)
          .toString();
    }

    static Optional<RecordInfo> parse(final String line) {

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
      new HadoopMetrics2Markdown(defaultInFile, "/tmp/sample.md");
    }
  }
}
