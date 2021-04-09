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
package org.apache.accumulo.core.data;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A strongly typed representation of an instance ID. This class cannot be used to create an
 * instance ID, but does provide the instance ID string wrapped with a stronger type. The
 * constructor for this class will throw an error if the canonical parameter is null.
 *
 * @since 2.1.0
 */
public class InstanceId extends AbstractId<InstanceId> {
  // cache is for canonicalization/deduplication of created objects,
  // StrongReference are used because there should be few active instance ids at any point in time.
  static final Map<String,InstanceId> cache = new ConcurrentHashMap<>();
  private static final long serialVersionUID = 1L;

  private InstanceId(String canonical) {
    super(canonical);
  }

  /**
   * Get a InstanceId object for the provided canonical string.
   *
   * @param canonical
   *          Namespace ID string
   * @return Instance.ID object
   */
  public static InstanceId of(final String canonical) {
    //TODO some things are currently using null as absence
    if(Objects.isNull(canonical)){
      return new InstanceId("");
    }
    return cache.computeIfAbsent(canonical, k -> new InstanceId(canonical));
  }
}
