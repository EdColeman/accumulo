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
package org.apache.accumulo.server.util;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf2.PropCacheId;
import org.apache.accumulo.server.conf2.PropStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropUtil {

  private static final Logger log = LoggerFactory.getLogger(PropUtil.class);

  /**
   * Utility class with static methods - prevent direct construction.
   */
  private PropUtil() {}

  public static boolean setProperty(final ServerContext context, final PropCacheId propCacheId,
      final String property, final String value) throws PropStoreException {

    if (!isPropertyValid(property, value))
      return false;

    return false;
  }

  public static void removeProperty(final ServerContext context, final PropCacheId propCacheId,
      final String property) {

  }

  public static boolean isPropertyValid(String property, String value) {
    Property p = Property.getPropertyByKey(property);
    return (p == null || p.getType().isValidFormat(value))
        && Property.isValidTablePropertyKey(property);
  }

}
