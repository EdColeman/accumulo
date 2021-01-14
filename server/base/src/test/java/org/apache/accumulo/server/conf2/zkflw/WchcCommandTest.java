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
package org.apache.accumulo.server.conf2.zkflw;

import java.util.List;
import java.util.regex.Pattern;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WchcCommandTest {

  private static final Logger log = LoggerFactory.getLogger(WchcCommandTest.class);

  @Test
  public void watcherSnapshot() {

    WchcCommand wchcCommand = new WchcCommand("localhost", 2181);

    wchcCommand.sendZkWchcCmd();

    List<WchcCommand.SplitPath> paths =
        wchcCommand.filterWchcOutput(Pattern.compile("/accumulo/.*/2/conf2.*"));

    log.info("Paths:{}", paths);
  }
}
