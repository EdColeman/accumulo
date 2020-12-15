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

import org.apache.accumulo.core.data.AbstractId;

public class ConfigurationCache implements Configuration {

  private final PropStore store;

  public ConfigurationCache(final PropStore store){
    this.store = store;
  }

  @Override
  public String getProperty(final String name){
    return "";
  }

  @Override
  public void setProperty(final String name, final String value){

  }

  private enum PropScope {
    DEFAULT,
    SYSTEM,
    NAMESPACE,
    TABLE,
    NONE
  }

  private static class PropKey {

    final AbstractId<?> id;
    final PropScope scope;
    final String name;

    public PropKey(final AbstractId<?> id, final PropScope scope, final String name){
      this.id = id;
      this.scope = scope;
      this.name = name;
    }
  }
}
