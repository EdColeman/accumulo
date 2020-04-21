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
package org.apache.accumulo.core.metrics;

import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DefaultMetricsRegistrationTest {

  private static final Logger log = LoggerFactory.getLogger(DefaultMetricsRegistrationTest.class);
  public static final String ENABLED_SUFFIX = ".enabled";

  @Test public void jarSearch() throws IOException {

    log.info("classpath: " + System.getProperty("java.class.path"));
    log.info("ext: " + System.getProperty("java.ext.dir"));
    log.info("boot: " + System.getProperty("jdk.boot.class.path.append"));

  }

  @Test public void x() {

    MyReg r = new MyReg();
    log.info("Cfg: {}", r);
    log.info("Cfg K: {}", r.lookup("X"));
    log.info("Cfg S: {}", Cfg.DEFAULT.step());
    log.info("Cfg H: {}", Cfg.DEFAULT.sayHello());
  }

  interface CfgParent {
    String get(String key);

    default String sayHello() {
      return "hello";
    }

    ;
  }

  interface Cfg extends CfgParent {
    Cfg DEFAULT = k -> "foo";

    default String step() {
      return "bar";
    }
  }

  class MyReg {

    private Cfg cfg;

    public MyReg() {
      this(Cfg.DEFAULT);
    }

    public MyReg(Cfg cfg) {
      this.cfg = cfg;
    }

    public String lookup(String key) {
      return cfg.get(key);
    }

    @Override public String toString() {
      return "MyReg{" + "cfg=" + cfg + '}';
    }
  }

  @Test public void config() throws IOException {

    String configFile = choosePropFile();
    Configuration config = loadConfigOrDefault(configFile);

    // access configuration properties
    Collection<String> x = findEnabled(config);

    log.info("Enabled: {}", x);

  }

  private Configuration loadConfigOrDefault(final String filename) {

    Configurations configs = new Configurations();

    if (Objects.isNull(filename)) {
      log.info("No configuration file provided, returning default (empty) configuration.");
      return new DefaultConfig();
    }

    try {

      return configs.properties(filename);

    } catch (ConfigurationException ex) {
      log.warn("No configuration file provided, returning default (empty) configuration.", ex);
      return new DefaultConfig();
    }
  }

  static class DefaultConfig extends AbstractConfiguration {

    @Override protected void addPropertyDirect(String s, Object o) {

    }

    @Override protected void clearPropertyDirect(String s) {

    }

    @Override protected Iterator<String> getKeysInternal() {
      return null;
    }

    @Override protected Object getPropertyInternal(String s) {
      return null;
    }

    @Override protected boolean isEmptyInternal() {
      return false;
    }

    @Override protected boolean containsKeyInternal(String s) {
      return false;
    }
  }
  private String choosePropFile() throws IOException {

    var METRICS_PROP_FILENAME = "accumulo.metrics.properties";
    var METRICS_TEST_PROP_FILENAME = "accumulo.metrics-test.properties";

    ClassLoader loader = AccumuloClassLoader.getClassLoader();

    URL configFile = loader.getResource(METRICS_PROP_FILENAME);
    URL configTestFile = loader.getResource(METRICS_TEST_PROP_FILENAME);

    if (Objects.nonNull(configTestFile)) {
      if (Objects.nonNull(configFile)) {
        log.info("Metrics test properties overriding file: {}", configFile.getPath());
      }
      log.info("Using metrics test properties: {}", configTestFile.getPath());
      return configTestFile.getFile();
    }

    if(Objects.nonNull(configFile)) {
      log.info("Using metrics properties file: {}", configFile.getPath());
      return configFile.getFile();
    }

    log.info("No metrics property file found on classpath");
    return null;
  }

  private Collection<String> findEnabled(final Configuration config) {

    Set<String> enabled = new TreeSet<>();

    Pattern p = Pattern.compile("^accumulo\\.metrics\\.(?<prefix>.*)\\.enabled$");

    Iterator<String> keys = config.getKeys();

    while (keys.hasNext()) {

      String key = keys.next().strip();
      Matcher m = p.matcher(key);

      if (m.matches()) {
        enabled.add(m.group("prefix"));
      }
    }

    return enabled;
  }

}
