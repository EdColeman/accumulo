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
  // private static final String encodingVer = "1.0";

  private Header header;

  // allow for deconflicting updates
  // private final Instant timestamp;

  // allow quick checking is data is current.
  // private int dataVersion;

  // used internally for know how to handle underlying bytes.
  // private final boolean compressed;

  private final Map<String,String> props = new HashMap<>();

  private static final DateTimeFormatter tsFormatter =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC));

  public PropEncodingV1(final int dataVersion, final boolean compressed, final Instant timestamp) {
    header = new Header(dataVersion, timestamp, compressed);
  }

  public PropEncodingV1(final byte[] bytes) {

    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis)) {

      header = new Header(dis);

      if (header.isCompressed()) {
        uncompressProps(bis);
      } else {
        readProps(dis);
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
  public Instant getTimestamp() {
    return header.getTimestamp();
  }

  @Override
  public int getDataVersion() {
    return header.getDataVersion();
  }

  public boolean isCompressed() {
    return header.isCompressed();
  }

  @Override
  public byte[] toBytes() {

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {

      int dataVersion = header.getDataVersion() + 1;

      header = new Header(dataVersion, Instant.now(), header.isCompressed());
      header.writeHeader(dos);

      if (header.isCompressed()) {
        compressProps(bos);
      } else {
        writeProps(dos);
      }
      dos.flush();
      return bos.toByteArray();

    } catch (IOException ex) {
      throw new IllegalStateException("Encountered error serializing properties", ex);
    }
  }

  private void writeProps(final DataOutputStream dos) throws IOException {

    dos.writeInt(props.size());

    props.forEach((k, v) -> writeKV(k, v, dos));

    dos.flush();
  }

  private void compressProps(final ByteArrayOutputStream bos) {

    try (GZIPOutputStream gzipOut = new GZIPOutputStream(bos);
        DataOutputStream dos = new DataOutputStream(gzipOut)) {

      writeProps(dos);

      gzipOut.flush();
      gzipOut.finish();

    } catch (IOException ex) {
      throw new IllegalStateException("Encountered error compressing properties", ex);
    }
  }

  private void readProps(final DataInputStream dis) throws IOException {

    int items = dis.readInt();

    for (int i = 0; i < items; i++) {
      Map.Entry<String,String> e = readKV(dis);
      props.put(e.getKey(), e.getValue());
    }
  }

  private void uncompressProps(final ByteArrayInputStream bis) throws IOException {

    try (GZIPInputStream gzipIn = new GZIPInputStream(bis);
        DataInputStream dis = new DataInputStream(gzipIn)) {
      readProps(dis);
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
    sb.append("encoding=").append(header.getEncodingVer());
    pretty(prettyPrint, sb);
    sb.append("dataVersion=").append(header.getDataVersion());
    pretty(prettyPrint, sb);
    sb.append("timestamp=").append(tsFormatter.format(header.getTimestamp()));
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

  /**
   * Serialization metadata. This data should be written / appear in the encoded bytes first so that
   * decisions can be made that may make deserilization unnecessary.
   *
   * The header values are:
   * <ul>
   * <li>encodingVersion - allows for future changes to the encoding schema</li>
   * <li>dataVersion - allows for quick comparison by comparing versions numbers</li>
   * <li>timestamp - could allow for deconfliction of concurrent updates</li>
   * <li>compressed - when true, the rest of the payload is compressed</li>
   * </ul>
   */
  private static class Header {

    private final String encodingVer = "1.0";
    private final int dataVersion;
    private final Instant timestamp;
    private final boolean compressed;

    public Header(final int dataVersion, final Instant timestamp, final boolean compressed) {
      this.dataVersion = dataVersion;
      this.timestamp = timestamp;
      this.compressed = compressed;
    }

    public Header(final DataInputStream dis) throws IOException {

      // temporary - would need to change if multiple, compatible versions are developed.
      String ver = dis.readUTF();
      if (encodingVer.compareTo(ver) != 0) {
        throw new IllegalStateException(
            "Invalid encoding version " + ver + ", expected " + encodingVer);
      }
      dataVersion = dis.readInt();
      timestamp = tsFormatter.parse(dis.readUTF(), Instant::from);
      compressed = dis.readBoolean();
    }

    public String getEncodingVer() {
      return encodingVer;
    }

    public int getDataVersion() {
      return dataVersion;
    }

    public Instant getTimestamp() {
      return timestamp;
    }

    public boolean isCompressed() {
      return compressed;
    }

    public void writeHeader(final DataOutputStream dos) throws IOException {
      dos.writeUTF(encodingVer);
      dos.writeInt(dataVersion);
      dos.writeUTF(tsFormatter.format(timestamp));
      dos.writeBoolean(compressed);
    }

    @Override
    public String toString() {
      return "Header{" + "encodingVer='" + encodingVer + '\'' + ", dataVersion=" + dataVersion
          + ", timestamp=" + timestamp + ", compressed=" + compressed + '}';
    }
  }

}
