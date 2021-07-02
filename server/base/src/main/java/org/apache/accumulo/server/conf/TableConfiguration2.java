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

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

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
import org.apache.accumulo.server.conf2.PropCacheId1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableConfiguration2 extends ZooBasedConfiguration {

  private static final Logger log = LoggerFactory.getLogger(TableConfiguration2.class);

  private final EnumMap<IteratorScope,Deriver<ParsedIteratorConfig>> iteratorConfig;

  private final Deriver<ScanDispatcher> scanDispatchDeriver;
  private final Deriver<CompactionDispatcher> compactionDispatchDeriver;

  public TableConfiguration2(ServerContext context, TableId tableId,
      NamespaceConfiguration2 parent) {
    super(log, context, PropCacheId1.forTable(context, tableId), parent);

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

  private static ScanDispatcher createScanDispatcher(AccumuloConfiguration conf,
      ServerContext context, TableId tableId) {
    ScanDispatcher newDispatcher = Property.createTableInstanceFromPropertyName(conf,
        Property.TABLE_SCAN_DISPATCHER, ScanDispatcher.class, null);

    Map<String,String> opts =
        conf.getAllPropertiesWithPrefixStripped(Property.TABLE_SCAN_DISPATCHER_OPTS);

    newDispatcher.init(new ScanDispatcher.InitParameters() {

      private final ServiceEnvironment senv = new ServiceEnvironmentImpl(context);

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
        return senv;
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

      private final ServiceEnvironment senv = new ServiceEnvironmentImpl(context);

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
        return senv;
      }
    });

    return newDispatcher;
  }

  public TableId getTableId() {
    return getCacheId().getTableId().orElseThrow(
        () -> new IllegalArgumentException("Invalid request fro table id on " + getCacheId()));
  }

  @Override
  public String getValid(Property property) {
    return super.getValid(property);
  }

  // @Override
  // public void getProperties(Map<String,String> props, Predicate<String> filter) {
  // getPropCacheAccessor().getProperties(props, getPath(), filter, parent, null);
  // }

  /**
   * Gets the parent configuration of this configuration.
   *
   * @return parent configuration
   */
  public NamespaceConfiguration2 getParentConfiguration() {
    return (NamespaceConfiguration2) getParent();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }

  public ParsedIteratorConfig getParsedIteratorConfig(IteratorScope scope) {
    return iteratorConfig.get(scope).derive();
  }

  public ScanDispatcher getScanDispatcher() {
    return scanDispatchDeriver.derive();
  }

  public CompactionDispatcher getCompactionDispatcher() {
    return compactionDispatchDeriver.derive();
  }

  public static class ParsedIteratorConfig {
    private final List<IterInfo> tableIters;
    private final Map<String,Map<String,String>> tableOpts;
    private final String context;

    private ParsedIteratorConfig(List<IterInfo> ii, Map<String,Map<String,String>> opts,
        String context) {
      this.tableIters = List.copyOf(ii);
      tableOpts = opts.entrySet().stream()
          .collect(Collectors.toUnmodifiableMap(Entry::getKey, e -> Map.copyOf(e.getValue())));
      this.context = context;
    }

    public List<IterInfo> getIterInfo() {
      return tableIters;
    }

    public Map<String,Map<String,String>> getOpts() {
      return tableOpts;
    }

    public String getServiceEnv() {
      return context;
    }
  }
}
