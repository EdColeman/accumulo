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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.metrics.MetricsServiceEnvironment;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;

import com.google.common.net.HostAndPort;

import io.micrometer.core.instrument.Tag;

public class MetricsServiceEnvironmentImpl extends ServiceEnvironmentImpl
    implements MetricsServiceEnvironment {

  private final ServerContext context;
  private final String appName;
  private final String serviceHostAndPort;
  private final InstanceId instanceId;

  private final List<Tag> commonTags;

  private final boolean micrometerEnabled;
  private final boolean jvmMetricsEnabled;
  private final String metricsFactoryName;

  public MetricsServiceEnvironmentImpl(final ServerContext context, final String appName,
      final String serviceHostAndPort) {
    super(context);
    this.context = context;
    this.appName = appName;
    this.serviceHostAndPort = serviceHostAndPort;
    this.instanceId = context.getInstanceID();

    AccumuloConfiguration config = context.getConfiguration();
    micrometerEnabled = config.getBoolean(Property.GENERAL_MICROMETER_ENABLED);
    jvmMetricsEnabled = config.getBoolean(Property.GENERAL_MICROMETER_JVM_METRICS_ENABLED);
    metricsFactoryName = config.get(Property.GENERAL_MICROMETER_FACTORY);

    String processName = appName;
    String serviceInstance = System.getProperty(METRICS_SERVICE_PROP_KEY, "");
    if (!serviceInstance.isBlank()) {
      processName += serviceInstance;
    }

    List<Tag> tags = new ArrayList<>();
    tags.add(Tag.of("instance.name", getInstanceId().canonical()));
    tags.add(Tag.of("process.name", processName));

    HostAndPort hostAndPort = HostAndPort.fromString(serviceHostAndPort);
    if (!hostAndPort.getHost().isEmpty()) {
      tags.add(Tag.of("host", hostAndPort.getHost()));
    }
    if (hostAndPort.getPort() > 0) {
      tags.add(Tag.of("port", Integer.toString(hostAndPort.getPort())));
    }
    commonTags = Collections.unmodifiableList(tags);
  }

  @Override
  public ServerContext getContext() {
    return context;
  }

  @Override
  public boolean isMicrometerEnabled() {
    return micrometerEnabled;
  }

  @Override
  public boolean isJvmMetricsEnabled() {
    return jvmMetricsEnabled;
  }

  @Override
  public String getMetricsFactoryName() {
    return metricsFactoryName;
  }

  @Override
  public String getTableName(TableId tableId) throws TableNotFoundException {
    throw new UnsupportedOperationException("metrics per table not supported");
  }

  @Override
  public <T> T instantiate(String className, Class<T> base) throws ReflectiveOperationException {
    return ConfigurationTypeHelper.getClassInstance(null, className, base);
  }

  @Override
  public <T> T instantiate(TableId tableId, String className, Class<T> base)
      throws ReflectiveOperationException {
    throw new UnsupportedOperationException("metrics per table not supported");
  }

  @Override
  public Configuration getConfiguration() {
    return (Configuration) context.getConfiguration();
  }

  @Override
  public Configuration getConfiguration(TableId tableId) {
    throw new UnsupportedOperationException("metrics per table not supported");
  }

  @Override
  public String getAppName() {
    return appName;
  }

  @Override
  public String getServiceHostAndPort() {
    return serviceHostAndPort;
  }

  @Override
  public InstanceId getInstanceId() {
    return instanceId;
  }

  @Override
  public List<Tag> getCommonTags() {
    return commonTags;
  }

}
