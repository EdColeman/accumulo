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
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.*;
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

  @Test public void config() throws IOException {

    Configurations configs = new Configurations();

    ClassLoader loader = AccumuloClassLoader.getClassLoader();
    URL configFile = loader.getResource("accumulo.metrics.properties");

    try {
      Configuration config = configs.properties(configFile.getFile());
      // access configuration properties
      Collection<String> x = findEnabled(config);

      log.info("Enabled: {}", x);

    } catch (ConfigurationException ex) {
      log.error("Could not read properties", ex);
    }
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
