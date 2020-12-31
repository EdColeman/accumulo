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
package org.apache.accumulo.server.conf;

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.IterConfigUtil;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher;
import org.apache.accumulo.core.spi.scan.ScanDispatcher;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.conf2.CacheId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableConfiguration extends ZooBasedConfiguration {

  private static final Logger log = LoggerFactory.getLogger(TableConfiguration.class);

  private final TableId tableId;

  private final EnumMap<IteratorScope,Deriver<ParsedIteratorConfig>> iteratorConfig;
  private final Deriver<ScanDispatcher> scanDispatchDeriver;
  private final Deriver<CompactionDispatcher> compactionDispatchDeriver;

  // should only instantiate once per table, in ServerConfigurationFactory
  protected TableConfiguration(ServerContext context, TableId tableId,
      NamespaceConfiguration parent) {
    super(log, context, CacheId.forTable(context, tableId), parent);
    this.tableId = requireNonNull(tableId);

    iteratorConfig = new EnumMap<>(IteratorScope.class);
    for (IteratorScope scope : IteratorScope.values()) {
      iteratorConfig.put(scope, newDeriver(conf -> {
        Map<String,Map<String,String>> allOpts = new HashMap<>();
        List<IterInfo> iters =
            IterConfigUtil.parseIterConf(scope, Collections.emptyList(), allOpts, conf);
        return new ParsedIteratorConfig(iters, allOpts, ClassLoaderUtil.tableContext(conf));
      }));
    }

    scanDispatchDeriver = newDeriver(conf -> createScanDispatcher(conf, context, tableId));
    compactionDispatchDeriver =
        newDeriver(conf -> createCompactionDispatcher(conf, context, tableId));
  }

  public TableId getTableId() {
    return tableId;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "(TableId:" + getTableId() + ")";
  }

  public ParsedIteratorConfig getParsedIteratorConfig(IteratorScope scope) {
    return iteratorConfig.get(scope).derive();
  }

  private static ScanDispatcher createScanDispatcher(AccumuloConfiguration conf,
      ServerContext context, TableId tableId) {
    ScanDispatcher newDispatcher = Property.createTableInstanceFromPropertyName(conf,
        Property.TABLE_SCAN_DISPATCHER, ScanDispatcher.class, null);

    Map<String,String> opts =
        conf.getAllPropertiesWithPrefixStripped(Property.TABLE_SCAN_DISPATCHER_OPTS);

    newDispatcher.init(new ScanDispatcher.InitParameters() {
      @Override
      public TableId getTableId() {
        return tableId;
      }

      @Override
      public Map<String,String> getOptions() {
        return opts;
      }

      @Override
      public ServiceEnvironment getServiceEnv() {
        return new ServiceEnvironmentImpl(context);
      }
    });

    return newDispatcher;
  }

  private static CompactionDispatcher createCompactionDispatcher(AccumuloConfiguration conf,
      ServerContext context, TableId tableId) {
    CompactionDispatcher newDispatcher = Property.createTableInstanceFromPropertyName(conf,
        Property.TABLE_COMPACTION_DISPATCHER, CompactionDispatcher.class, null);

    Map<String,String> opts =
        conf.getAllPropertiesWithPrefixStripped(Property.TABLE_COMPACTION_DISPATCHER_OPTS);

    newDispatcher.init(new CompactionDispatcher.InitParameters() {
      @Override
      public TableId getTableId() {
        return tableId;
      }

      @Override
      public Map<String,String> getOptions() {
        return opts;
      }

      @Override
      public ServiceEnvironment getServiceEnv() {
        return new ServiceEnvironmentImpl(context);
      }
    });

    return newDispatcher;
  }

  public ScanDispatcher getScanDispatcher() {
    return scanDispatchDeriver.derive();
  }

  public CompactionDispatcher getCompactionDispatcher() {
    return compactionDispatchDeriver.derive();
  }
}
