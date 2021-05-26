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
package org.apache.accumulo.server.confRewrite.zk;

import org.apache.accumulo.server.conf2.CacheId;
import org.apache.accumulo.server.conf2.codec.PropEncoding;
import org.apache.zookeeper.data.Stat;

/**
 * Defines ZooKeeper property management responsibilities to read, write and delete property nodes
 * from zookeeper.
 */
public interface BackingStore {

  boolean createInStore(final CacheId id, PropEncoding props);

  PropEncoding readFromStore(final CacheId id, final Stat stat);

  Stat readZkStat(final CacheId id);

  void writeToStore(final CacheId id, PropEncoding props);

  void deleteFromStore(final CacheId id);

}
