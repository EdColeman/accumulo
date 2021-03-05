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
package org.apache.accumulo.test.config2.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

public class ExpectedProps {

  private final long lastUpdate;
  private final int expectedInt;
  private final boolean isUpdating;

  public ExpectedProps(long lastUpdate, final int expectedInt, final boolean isUpdating) {
    this.lastUpdate = lastUpdate;
    this.expectedInt = expectedInt;
    this.isUpdating = isUpdating;
  }

  public boolean isUpdating() {
    return isUpdating;
  }

  public int getExpectedInt() {
    return expectedInt;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ExpectedProps.class.getSimpleName() + "[", "]")
        .add("lastUpdate=" + lastUpdate).add("expected=" + expectedInt)
        .add("isUpdating=" + isUpdating).toString();
  }

  public Map<String,String> toMap() {
    Map<String,String> props = new HashMap<>();
    props.put(ExpectedValues.PROP_INT_NAME, Integer.toString(expectedInt));
    props.put(ExpectedValues.PROP_UPDATE_NAME, Long.toString(lastUpdate));
    return props;
  }
}
