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
package org.apache.accumulo.core.conf.zkprops;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.AbstractId;

public class ZkPropPath extends AbstractId<ZkPropPath> {

  private ZkPropPath(final String canonical) {

    super(canonical);

    if (!canonical.startsWith(Constants.ZROOT)) {
      throw new IllegalArgumentException("Zookeeper path should start with " + Constants.ZROOT
          + ", received: '" + canonical + "'");
    }
  }

  public static ZkPropPath of(final String path) {
    return new ZkPropPath(path);
  }

  public static Parts parse(ZkPropPath path) {
    return new Parts(Objects.requireNonNull(path, "Path cannot be null"));
  }

  public static class Parts {

    private final String instance;
    private final String base;
    private final String node;

    // the general form is /accumulo/[instance-id][base][id]
    private static final Pattern pathPattern =
        Pattern.compile("^/(?<root>\\w*)/(?<instanceId>\\w*)(?<base>/.*)*/(?<node>\\w*)$");

    private Parts(ZkPropPath path) {

      Matcher m = pathPattern.matcher(path.canonical());
      if (!m.matches() || m.groupCount() != 4) {
        throw new IllegalArgumentException("Invalid zookeeper path provided '" + path.canonical()
            + "' expected form /accumulo/instance/[path]/node");
      }

      instance = m.group("instanceId");
      base = m.group("base");
      node = m.group("node");
    }

    public String getInstance() {
      return instance;
    }

    public String getBase() {
      return base;
    }

    public String getNode() {
      return node;
    }
  }
}
