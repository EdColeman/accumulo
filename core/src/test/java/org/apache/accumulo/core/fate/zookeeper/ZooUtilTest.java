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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.jupiter.api.Test;

public class ZooUtilTest {

  private static final List<ACL> unsafePrivateAcl = new ArrayList<>(ZooDefs.Ids.CREATOR_ALL_ACL);
  private static final List<ACL> unsafePublicAcl = new ArrayList<>(unsafePrivateAcl);
  static {
    unsafePublicAcl.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
  }

  @Test
  public void mutableListTest() {

    try {
      validateACL(unsafePrivateAcl);
      validateACL(unsafePublicAcl);
    } catch (KeeperException.InvalidACLException ex) {
      fail("validation of mutable acl list failed", ex);
    }

  }

  @Test
  public void mutableListWithNullTest() {

    List<ACL> withNull = new ArrayList<>(unsafePublicAcl);
    withNull.add(null);

    assertThrows(KeeperException.InvalidACLException.class, () -> validateACL(withNull));
    assertThrows(KeeperException.InvalidACLException.class, () -> validateACLSafe(withNull));

  }

  @Test
  public void immutableListFailTest() {
    List<ACL> safePrivateAcl = List.copyOf(unsafePrivateAcl);
    List<ACL> safePublicAcl = List.copyOf(unsafePublicAcl);

    assertThrows(NullPointerException.class, () -> validateACL(safePrivateAcl));
    assertThrows(NullPointerException.class, () -> validateACL(safePublicAcl));
  }

  @Test
  public void immutableListPassTest() throws KeeperException.InvalidACLException {
    List<ACL> safePrivateAcl = List.copyOf(unsafePrivateAcl);
    List<ACL> safePublicAcl = List.copyOf(unsafePublicAcl);

    validateACLSafe(safePrivateAcl);
    validateACLSafe(safePublicAcl);
  }

  @Test
  public void immutableListTest() {
    List<ACL> safePrivateAcl = List.copyOf(unsafePrivateAcl);
    List<ACL> safePublicAcl = List.copyOf(unsafePublicAcl);

    assertThrows(NullPointerException.class, () -> validateACL(safePrivateAcl));
    assertThrows(NullPointerException.class, () -> validateACL(safePublicAcl));
  }

  /** ZooKeeper ACL validation code */
  /**
   * Validates the provided ACL list for null, empty or null value in it.
   *
   * @param acl ACL list
   * @throws KeeperException.InvalidACLException if ACL list is not valid
   */
  private void validateACL(List<ACL> acl) throws KeeperException.InvalidACLException {
    if (acl == null || acl.isEmpty() || acl.contains(null)) {
      throw new KeeperException.InvalidACLException();
    }
  }

  private void validateACLSafe(List<ACL> acl) throws KeeperException.InvalidACLException {
    if (acl == null || acl.isEmpty() || hasNull(acl)) {
      throw new KeeperException.InvalidACLException();
    }
  }

  /**
   * Test if a collection contains a null entry. This method avoids collection.contains(null) which
   * will throw an NPE when querying an ImmutableCollection for null using methods contains,
   * indexOf, lastIndexOf (JDK-8265905)
   *
   * @param acl a list of ACLs
   *
   * @return true if the list contains a null, false otherwise.
   */
  private boolean hasNull(List<ACL> acl) {
    for (ACL a : acl) {
      if (a == null) {
        return true;
      }
    }
    return false;
  }
}
