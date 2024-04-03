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
package org.apache.accumulo.core.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.accumulo.core.util.HostAndPort;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

public interface MetricsInfo {

  static List<Tag> serviceTags(final String instanceName, final String applicationName,
      final HostAndPort address) {
    List<Tag> tags = new ArrayList<>();
    if (instanceName != null && !instanceName.isEmpty()) {
      tags.add(MetricsInfo.instanceNameTag(instanceName));
    }
    if (applicationName != null && !applicationName.isEmpty()) {
      tags.add(MetricsInfo.processTag(applicationName));
    }
    if (address != null) {
      tags.addAll(MetricsInfo.addressTags(address));
    }
    return tags;
  }

  // common metrics tag names used as tag keys.
  static Tag instanceNameTag(final String instanceName) {
    Objects.requireNonNull(instanceName,
        "cannot create the tag without providing the instance name");
    return Tag.of("instance.name", instanceName);
  }

  static Tag processTag(final String processName) {
    Objects.requireNonNull(processName, "cannot create the tag without providing the process name");
    return Tag.of("process.name", processName);
  }

  // host and port from address host:port pair to allow filtering on components
  static List<Tag> addressTags(final HostAndPort hostAndPort) {
    Objects.requireNonNull(hostAndPort, "cannot create the tag without providing the hostAndPort");
    List<Tag> tags = new ArrayList<>(2);
    tags.add(Tag.of("host", hostAndPort.getHost()));
    int port = hostAndPort.getPort();
    if (port != 0) {
      tags.add(Tag.of("port", Integer.toString(hostAndPort.getPort())));
    }
    return Collections.unmodifiableList(tags);
  }

  boolean metricsEnabled();

  void addProcessTags(final List<Tag> updates);

  void addCommonTags(final List<Tag> updates);

  Collection<Tag> getProcessTags();

  Collection<Tag> getCommonTags();

  void init();

  void addRegistry(MeterRegistry registry);

  void addMetricsProducers(MetricsProducer... producer);

  MeterRegistry getRegistry();
}
