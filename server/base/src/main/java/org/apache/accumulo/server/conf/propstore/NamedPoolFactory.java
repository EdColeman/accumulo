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
package org.apache.accumulo.server.conf.propstore;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamedPoolFactory {

  private static final Logger log = LoggerFactory.getLogger(SentinelNode.class);

  private static final Pattern nodeNamePattern = Pattern.compile("^/(.+/)*(.+)$");

  public static BasicThreadFactory createNamedThreadFactory(String zooPath) {
    return new BasicThreadFactory.Builder().namingPattern(extractNodeName(zooPath) + "-%d")
        .daemon(false).priority(Thread.NORM_PRIORITY).build();
  }

  /**
   * Returns the node name from a zookeeper path.
   *
   * @param path
   *          zookeeper path
   * @return the node name
   */
  private static String extractNodeName(final String path) {

    Matcher matcher = nodeNamePattern.matcher(path);
    if (matcher.matches()) {

      log.trace("path: {} -> {}", path, matcher.group(matcher.groupCount()));

      return matcher.group(matcher.groupCount());
    }
    return "NoNode";
  }

}
