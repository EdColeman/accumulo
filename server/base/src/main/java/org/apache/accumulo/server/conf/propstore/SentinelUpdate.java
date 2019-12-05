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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;

import org.apache.accumulo.core.data.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class SentinelUpdate {

  private static final Logger log = LoggerFactory.getLogger(SentinelUpdate.class);

  private static final Gson gson = new Gson();

  public enum Reasons {
    NEW_TABLE, UPDATE, DELETE_TABLE, UNKNOWN
  }

  private final Reasons reason;
  private final TableId tableId;

  public SentinelUpdate(final Reasons reason, final TableId tableId) {
    this.reason = reason;
    this.tableId = tableId;
  }

  // no-args constructor for gson serialization
  @SuppressWarnings("unused")
  private SentinelUpdate() {
    reason = Reasons.UNKNOWN;
    tableId = null;
  }

  public Reasons getReason() {
    return reason;
  }

  public TableId getTableId() {
    return tableId;
  }

  public byte[] toJson() {
    return gson.toJson(this).getBytes(UTF_8);
  }

  public static SentinelUpdate fromJson(final byte[] bytes) {
    Objects.requireNonNull(bytes, "Cannot deserialize null bytes");

    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        InputStreamReader reader = new InputStreamReader(bis)) {

      return gson.fromJson(reader, SentinelUpdate.class);
    } catch (IOException ex) {
      log.trace("Failed to deserialize {}", new String(bytes, UTF_8));
      throw new IllegalStateException("Failed to deserialize properties", ex);
    }
  }

  @Override
  public String toString() {
    return "SentinelUpdate{" + ", tableId=" + tableId + '\'' + ", reason='" + reason + '}';
  }
}
