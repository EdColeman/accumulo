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
package org.apache.accumulo.test.compaction;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.apache.accumulo.core.util.compaction.CompactionPlannerInitParams;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("removal")
public class CompactionPlanSimIT {
    private static final Logger LOG = LoggerFactory.getLogger(CompactionPlanSimIT.class);
    private static final CompactionServiceId csid = CompactionServiceId.of("cs1");

    @Test
    public void buildPlan(){
        ConfigurationCopy aconf = new ConfigurationCopy(DefaultConfiguration.getInstance());
        aconf.set(Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + "cs1.planner.opts.maxOpen", "15");
        ConfigurationImpl config = new ConfigurationImpl(aconf);

        String executors = "[{'name':'small','type': 'internal','maxSize':'32M','numThreads':1},"
                + "{'name':'medium','type': 'internal','maxSize':'128M','numThreads':2},"
                + "{'name':'large','type': 'internal','maxSize':'512M','numThreads':3},"
                + "{'name':'huge','type': 'internal','numThreads':4}]";

        var dcp = createPlanner(config,executors);
        var all = createCFs("F1", "128M", "F2", "129M", "F3", "130M", "F4", "131M", "F5", "132M");
        var params = createPlanningParams(all, all, Set.of(), 2, CompactionKind.SYSTEM);
        var plan = dcp.makePlan(params);

        // should only compact files less than max size
        var job = plan.getJobs();

        LOG.info("job: {}", job);
    }
    private static DefaultCompactionPlanner createPlanner(ServiceEnvironment.Configuration conf, String executors) {
        DefaultCompactionPlanner planner = new DefaultCompactionPlanner();

        ServiceEnvironment senv = EasyMock.createMock(ServiceEnvironment.class);
        EasyMock.expect(senv.getConfiguration()).andReturn(conf).anyTimes();
        EasyMock.replay(senv);

        Map<String,String> options = new HashMap<>();
        var initParams = new CompactionPlannerInitParams(csid, options, senv);
        options.put("executors", executors.replaceAll("'", "\""));

        planner.init(initParams);
        return planner;
    }
    private static Set<CompactableFile> createCFs(String... namesSizePairs) {
        Set<CompactableFile> files = new HashSet<>();

        for (int i = 0; i < namesSizePairs.length; i += 2) {
            String name = namesSizePairs[i];
            long size = ConfigurationTypeHelper.getFixedMemoryAsBytes(namesSizePairs[i + 1]);
            try {
                files.add(CompactableFile
                        .create(new URI("hdfs://fake/accumulo/tables/1/t-0000000z/" + name + ".rf"), size, 0));
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }

        return files;
    }

}
