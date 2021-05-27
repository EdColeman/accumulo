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
package org.apache.accumulo.server.conf2;

import org.apache.zookeeper.KeeperException;

public class PropStoreException extends Exception {

  private static final long serialVersionUID = 1;
  private final REASON_CODE code;

  public PropStoreException(final String message, Exception ex) {
    super(message, ex);
    if (ex instanceof KeeperException) {
      code = getZkReason((KeeperException) ex);
    } else if (ex instanceof InterruptedException) {
      Thread.currentThread().interrupt();
      code = REASON_CODE.INTERRUPT;
    } else {
      code = REASON_CODE.OTHER;
    }
  }

  public PropStoreException(final REASON_CODE code, final String message, final Throwable err) {
    super(message, err);
    this.code = code;
  }

  public static REASON_CODE getZkReason(KeeperException ex) {
    switch (ex.code()) {
      case NONODE:
        return REASON_CODE.NO_ZK_NODE;
      case BADVERSION:
        return REASON_CODE.UNEXPECTED_ZK_VERSION;
      default:
        return REASON_CODE.ZK_ERROR;
    }
  }

  public REASON_CODE getCode() {
    return code;
  }

  public enum REASON_CODE {
    INVALID_PROPERTY, UNEXPECTED_ZK_VERSION, NO_ZK_NODE, ZK_ERROR, INTERRUPT, OTHER
  }
}
