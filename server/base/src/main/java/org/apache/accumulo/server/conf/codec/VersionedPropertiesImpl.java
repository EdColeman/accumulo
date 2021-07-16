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
package org.apache.accumulo.server.conf.codec;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class VersionedPropertiesImpl implements VersionedProperties {

  private final Map<String,String> props;
  private VersionInfo versionInfo;

  public VersionedPropertiesImpl(final VersionInfo versionInfo) {
    this(versionInfo.getDataVersion(), versionInfo.getTimestamp(), new HashMap<>());
  }

  public VersionedPropertiesImpl(final VersionInfo versionInfo, final Map<String,String> props) {
    this(versionInfo.getDataVersion(), versionInfo.getTimestamp(), props);
  }

  /**
   * Instantiate an instance.
   *
   * @param dataVersion should match current zookeeper dataVersion, or a negative value for initial instance.
   * @param timestamp   timestamp for the data.
   */
  public VersionedPropertiesImpl(final int dataVersion, final Instant timestamp,
      final Map<String,String> props) {
    versionInfo =
        new VersionInfo.Builder().withDataVersion(dataVersion).withTimestamp(timestamp).build();
    this.props = new HashMap<>(props);
  }

  @Override public void addProperty(String k, String v) {
    props.put(k, v);
  }

  @Override public void addProperties(Map<String,String> properties) {
    if (Objects.nonNull(properties)) {
      props.putAll(properties);
    }
  }

  @Override public String getProperty(final String key) {
    return props.get(key);
  }

  @Override public Map<String,String> getAllProperties() {
    return Collections.unmodifiableMap(props);
  }

  @Override public String removeProperty(final String key) {
    return props.remove(key);
  }

  @Override public Instant getTimestamp() {
    return versionInfo.getTimestamp();
  }

  @Override public int getDataVersion() {
    return versionInfo.getDataVersion();
  }

  @Override public int getExpectedVersion() {
    return Math.max(0, getDataVersion() - 1);
  }

  @Override public String print(boolean prettyPrint) {
    StringBuilder sb = new StringBuilder();

    sb.append("dataVersion=").append(versionInfo.getDataVersion());
    pretty(prettyPrint, sb);
    sb.append("timestamp=").append(versionInfo.getTimestampISO());
    pretty(prettyPrint, sb);

    Map<String,String> sorted = new TreeMap<>(props);
    sorted.forEach((k, v) -> {
      if (prettyPrint) {
        sb.append("  ");
      }
      sb.append(k).append("=").append(v);
      pretty(prettyPrint, sb);
    });
    return sb.toString();
  }

  @Override public void updateVersionInfo(VersionInfo versionInfo) {
    this.versionInfo = versionInfo;
  }

  private void pretty(final boolean prettyPrint, final StringBuilder sb) {
    sb.append(prettyPrint ? "\n" : ", ");
  }

}
