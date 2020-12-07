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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class PropEncodingV1 implements PropEncoding {

  // allow for future expansion to support encoding migration.
  private static final String encodingVer = "1.0";

  // allow for deconflicting updates
  private Instant timestamp;

  // allow quick checking is data is current.
  private int dataVersion;

  // used internally for know how to handle underlying bytes.
  private final boolean compressed;

  private final Map<String,String> props = new HashMap<>();

  private static final DateTimeFormatter tsFormatter =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC));

  public PropEncodingV1(final int dataVersion, final boolean compressed, final Instant timestamp) {
    this.dataVersion = dataVersion;
    this.compressed = compressed;
    this.timestamp = timestamp;
  }

  public PropEncodingV1(final byte[] bytes) {

    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis)) {

      // temporary - would need to change if multiple, compatible versions are developed.
      String encodedVer = dis.readUTF();
      if (encodingVer.compareTo(encodedVer) != 0) {
        throw new IllegalStateException("Invalid encoding version " + encodedVer);
      }

      compressed = dis.readBoolean();

      if (compressed) {
        fromCompressed(bis);
      } else {
        readBytes(dis);
      }

    } catch (IOException ex) {
      throw new IllegalStateException("Encountered error deserializing properties", ex);
    }
  }

  @Override
  public void addProperty(String k, String v) {
    props.put(k, v);
  }

  @Override
  public String getProperty(final String key) {
    return props.get(key);
  }

  @Override
  public String getEncodingVer() {
    return encodingVer;
  }

  @Override
  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public int getDataVersion() {
    return dataVersion;
  }

  public boolean isCompressed() {
    return compressed;
  }

  @Override
  public byte[] toBytes() {

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {

      dos.writeUTF(encodingVer);
      dos.writeBoolean(compressed);

      dataVersion++;

      if (compressed) {
        bos.write(toCompressed());
      } else {
        writeData(dos);
      }
      dos.flush();
      return bos.toByteArray();

    } catch (IOException ex) {
      throw new IllegalStateException("Encountered error serializing properties", ex);
    }
  }

  private void writeData(final DataOutputStream dos) throws IOException {
    dos.writeInt(dataVersion);
    dos.writeUTF(tsFormatter.format(timestamp));

    dos.writeInt(props.size());

    props.forEach((k, v) -> writeKV(k, v, dos));

    dos.flush();
  }

  private byte[] toCompressed() {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {

      writeData(dos);

      ByteArrayOutputStream bos2 = new ByteArrayOutputStream();
      GZIPOutputStream gzipOut = new GZIPOutputStream(bos2);

      gzipOut.write(bos.toByteArray());

      gzipOut.close();
      return bos2.toByteArray();
    } catch (IOException ex) {
      throw new IllegalStateException("Encountered error compressing properties", ex);
    }
  }

  private void readBytes(final DataInputStream dis) throws IOException {

    dataVersion = dis.readInt();
    timestamp = tsFormatter.parse(dis.readUTF(), Instant::from);

    int items = dis.readInt();
    for (int i = 0; i < items; i++) {
      Map.Entry<String,String> e = readKV(dis);
      props.put(e.getKey(), e.getValue());
    }

  }

  private void fromCompressed(final ByteArrayInputStream bis) throws IOException {

    try (GZIPInputStream gzipIn = new GZIPInputStream(bis);
        DataInputStream dis = new DataInputStream(gzipIn)) {
      readBytes(dis);
    }
  }

  private void writeKV(final String k, final String v, final DataOutputStream dos) {
    try {
      dos.writeUTF(k);
      dos.writeUTF(v);
    } catch (IOException ex) {
      throw new IllegalStateException(
          String.format("Exception excountered writeing props k:'%s', v:'%s", k, v), ex);
    }
  }

  private Map.Entry<String,String> readKV(final DataInputStream dis) {
    try {
      String k = dis.readUTF();
      String v = dis.readUTF();
      return new AbstractMap.SimpleEntry<>(k, v);

    } catch (IOException ex) {
      throw new IllegalStateException("Could not read property key value pair", ex);
    }
  }

  @Override
  public String print(boolean prettyPrint) {
    StringBuilder sb = new StringBuilder();
    sb.append("encoding=").append(encodingVer);
    pretty(prettyPrint, sb);
    sb.append("dataVersion=").append(dataVersion);
    pretty(prettyPrint, sb);
    sb.append("timestamp=").append(tsFormatter.format(timestamp));
    pretty(prettyPrint, sb);
    props.forEach((k, v) -> {
      if (prettyPrint) {
        sb.append("  ");
      }
      sb.append(k).append("=").append(v);
      pretty(prettyPrint, sb);
    });
    return sb.toString();
  }

  private void pretty(final boolean prettyPrint, final StringBuilder sb) {
    if (prettyPrint) {
      sb.append("\n");
    } else {
      sb.append(", ");
    }
  }

}
