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
package org.apache.accumulo.server.conf.propstore;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.zookeeper.data.Stat;

public class PropTestData {

  /**
   * Generate a byte[] of json encoded table properties.
   *
   * @return json encoded table props
   */
  public static byte[] getTableBytes(final TableId tableId, final Stat stat) {

    ZkMap m = new ZkMap(tableId, stat.getVersion());

    m.set(Property.TABLE_BLOOM_ENABLED, "true");
    m.set(Property.TABLE_FILE_MAX, "99");

    return m.toJson();
  }

}
