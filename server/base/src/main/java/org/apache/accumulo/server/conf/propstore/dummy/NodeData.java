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
package org.apache.accumulo.server.conf.propstore.dummy;

import org.apache.accumulo.server.conf.propstore.ZooFunc;
import org.apache.zookeeper.data.Stat;

/**
 * Immutable class that contains node Stat and the data byte[] from a zookeeper node.
 */
public class NodeData {

  private final Stat stat;
  private final byte[] data;

  public NodeData(final Stat stat, final byte[] data) {
    this.stat = stat;
    this.data = data;
  }

  public static NodeData empty() {
    return new NodeData(new Stat(), new byte[0]);
  }

  public boolean isEmpty() {
    if (stat.getPzxid() == 0 && data.length == 0) {
      return true;
    }
    return false;
  }

  public Stat getStat() {
    return stat;
  }

  public byte[] getData() {
    return data;
  }

  @Override
  public String toString() {
    return "NodeData{" + "stat=" + ZooFunc.prettyStat(stat) + ", data length=" + data.length + '}';
  }
}
