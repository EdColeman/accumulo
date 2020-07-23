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

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PropNode manages a collection of properties for a single scope as a unit. Properties for a
 * scope are accessed with a Map&lt;String,String&gt; interface. The storage and serialization are
 * managed internally.
 */
public class PropNode {

  private static final Logger log = LoggerFactory.getLogger(PropNode.class);

  private final Map<String,String> props = new TreeMap<>();
  private PropId nodeId;

  private PropNode(final PropId nodeId) {
    this.nodeId = nodeId;
  }

  /**
   * default for serialization
   */
  PropNode() {}

  public PropId getNodeId() {
    return nodeId;
  }

  public void setProp(final String name, final String value) {
    props.put(name, value);
  }

  public Optional<String> getProp(final String name) {
    return Optional.ofNullable(props.get(name));
  }

  @Override
  public String toString() {
    return "PropNode{" + "id=" + nodeId + ", props=" + props + '}';
  }

  public static class Factory {

    public PropId id;

    public PropNode.Factory with(Consumer<PropNode.Factory> factory) {
      factory.accept(this);
      return this;
    }

    public PropNode create() {
      return new PropNode(id);
    }
  }
}
