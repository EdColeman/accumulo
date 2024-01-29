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
package org.apache.accumulo.server.metrics;

import static org.apache.accumulo.core.spi.metrics.MetricsServiceEnvironment.METRICS_SERVICE_PROP_KEY;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.server.ServerContext;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

import io.micrometer.core.instrument.Tag;

public class MetricsServiceEnvironmentImplTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(MetricsServiceEnvironmentImplTest.class);

  public static final String TEST_FACTORY_CLASSNAME =
      "org.apache.accumulo.server.metrics.MetricsTestFactory";

  @Test
  public void configPropTest() {
    Properties sysProp = System.getProperties();
    sysProp.setProperty(METRICS_SERVICE_PROP_KEY, "service1");
    System.setProperties(sysProp);

    ConfigurationCopy config = new ConfigurationCopy();
    config.set(Property.GENERAL_MICROMETER_ENABLED, "true");
    config.set(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED, "true");
    config.set(Property.GENERAL_MICROMETER_FACTORY.getKey(), TEST_FACTORY_CLASSNAME);

    InstanceId iid = InstanceId.of(UUID.randomUUID());

    ServerContext context = createMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(config).anyTimes();
    expect(context.getInstanceID()).andReturn(iid).anyTimes();

    replay(context);

    final String appName = "app1.";
    MetricsServiceEnvironmentImpl env =
        new MetricsServiceEnvironmentImpl(context, appName, "myhost1:12345");

    assertNotNull(env);
    assertTrue(env.isMicrometerEnabled());
    assertTrue(env.isJvmMetricsEnabled());
    assertEquals(TEST_FACTORY_CLASSNAME, env.getMetricsFactoryName());
    assertEquals(appName, env.getAppName());

    LOG.info("tags: {}", env.getCommonTags());
    List<Tag> commonTags = env.getCommonTags();
    Map<String,String> tagMap = new HashMap<>();
    commonTags.forEach(t -> tagMap.put(t.getKey(), t.getValue()));

    HostAndPort hostAndPort = HostAndPort.fromString(env.getServiceHostAndPort());
    LOG.info("tags: {}", tagMap);

    assertEquals(appName + "service1", tagMap.get("process.name"));
    assertEquals(iid.canonical(), tagMap.get("instance.name"));
    assertEquals(hostAndPort.getPort(), Integer.valueOf(tagMap.get("port")));
    assertEquals(hostAndPort.getHost(), tagMap.get("host"));

    verify(context);
  }
}
