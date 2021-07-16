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
package org.apache.accumulo.server.conf.codec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Serialization metadata. This data should be written / appear in the encoded bytes first so that
 * decisions can be made that may make deserilization unnecessary.
 * <p>
 * The header values are:
 * <ul>
 * <li>encodingVersion - allows for future changes to the encoding schema</li>
 * <li>dataVersion - allows for quick comparison by comparing versions numbers</li>
 * <li>timestamp - could allow for deconfliction of concurrent updates</li>
 * <li>compressed - when true, the rest of the payload is compressed</li>
 * </ul>
 */
public class VersionInfo {

  public static final int NO_VERSION = -2;

  private static final DateTimeFormatter tsFormatter =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC));

  private final int dataVersion;
  private final Instant timestamp;

  private VersionInfo(final int dataVersion, final Instant timestamp) {
    this.dataVersion = dataVersion;
    this.timestamp = timestamp;
  }

  public VersionInfo(final DataInputStream dis) throws IOException {
    dataVersion = dis.readInt();
    timestamp = tsFormatter.parse(dis.readUTF(), Instant::from);
  }

  public void encode(final DataOutputStream dos) throws IOException {
    dos.writeInt(dataVersion);
    dos.writeUTF(tsFormatter.format(timestamp));
  }

  /**
   * Get the data version - a negative value signals the data has not been written out. Avoids using
   * -1 because that has significance in ZooKeeper - writing a ZooKeeper node with a version of -1
   * disables the ZooKeeper expected version checking and just over writes the node.
   *
   * @return negative value if initial version, otherwise the current data version.
   */
  public int getDataVersion() {
    if (dataVersion < 0) {
      return NO_VERSION;
    }
    return dataVersion;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public String getTimestampISO() {
    return tsFormatter.format(timestamp);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", VersionInfo.class.getSimpleName() + "[", "]")
        .add("dataVersion=" + dataVersion).add("timestamp=" + getTimestampISO()).toString();
  }

  public static class Builder {

    private int dataVersion = NO_VERSION;
    private Instant timestamp;

    public VersionInfo build() {
      if (Objects.isNull(timestamp)) {
        timestamp = Instant.now();
      }
      return new VersionInfo(dataVersion, timestamp);
    }

    public Builder withTimestamp(final Instant timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder withDataVersion(final int dataVersion) {
      this.dataVersion = dataVersion;
      return this;
    }
  }
}
