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
package org.apache.accumulo.core.fate.zookeeper;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.InvalidACLException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZooUtilTest {
  Logger log = LoggerFactory.getLogger(ZooUtilTest.class);

  @Test
  public void checkImmutableAcl() throws KeeperException.InvalidACLException {

    final List<ACL> mutable = new ArrayList<>(ZooDefs.Ids.CREATOR_ALL_ACL);
    assertTrue(validateACL(mutable));

    try {
      final List<ACL> immutable = List.copyOf(ZooDefs.Ids.CREATOR_ALL_ACL);
      assertTrue(validateACL(immutable));
    } catch (Exception ex) {
      log.warn("validateAcls failed with exception", ex);
    }
  }

  /**
   * Copied from ZooKeeper lines 1105 - 1109
   */
  boolean validateACL(List<ACL> acl) throws KeeperException.InvalidACLException {
    if (acl == null || acl.isEmpty() || acl.contains((Object) null)) {
      throw new KeeperException.InvalidACLException();
    }
    return true;
  }
}
