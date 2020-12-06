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

import java.io.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

class PropEncodingV1 {

  private static final String encodingVer = "1.0";
  private final int dataVersion;
  private final Instant timestamp;

  private final Map<String,String> props = new HashMap<>();

  public PropEncodingV1() {
    this(1, Instant.now());
  }

  public PropEncodingV1(final int dataVersion, final Instant timestamp) {
    this.dataVersion = 1;
    this.timestamp = timestamp;
  }

  public void add(String k, String v) {
    props.put(k, v);
  }

  public byte[] toBytes() {

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {
      dos.writeUTF(encodingVer);

      bos.write(toCompressed());

//      dos.writeInt(dataVersion);
//      dos.writeUTF(tsFormatter.format(timestamp));
//
//      dos.writeInt(props.size());
//
//      props.forEach((k, v) -> writeKV(k, v, dos));
//
//      dos.flush();
//
      return bos.toByteArray();

    } catch (IOException ex) {
      throw new IllegalStateException("Encountered error serializing properties", ex);
    }
  }

  private byte[] toCompressed() {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {

      dos.writeInt(dataVersion);
      dos.writeUTF(tsFormatter.format(timestamp));

      dos.writeInt(props.size());

      props.forEach((k, v) -> writeKV(k, v, dos));

      dos.flush();

      ByteArrayOutputStream bos2 = new ByteArrayOutputStream();
      GZIPOutputStream gzipOut = new GZIPOutputStream(bos2);

      gzipOut.write(bos.toByteArray());

      gzipOut.close();
      return bos2.toByteArray();
    } catch (IOException ex) {
      throw new IllegalStateException("Encountered error compressing properties", ex);
    }
  }


  public static PropEncodingV1 fromBytes(final byte[] bytes) {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis)) {

      String encodingVer = dis.readUTF();

      PropEncodingV1 decoded = fromCompressed(bis);

//      int dataVersion = dis.readInt();
//      Instant timestamp = tsFormatter.parse(dis.readUTF(), Instant::from);
//
//      PropEncodingV1 decoded = new PropEncodingV1(dataVersion, timestamp);
//
//      int items = dis.readInt();
//      for (int i = 0; i < items; i++) {
//        Map.Entry<String,String> e = readKV(dis);
//        decoded.add(e.getKey(), e.getValue());
//      }

      return decoded;

    } catch (IOException ex) {
      throw new IllegalStateException("Encountered error deserializing properties", ex);
    }
  }

  private static PropEncodingV1 fromCompressed(final ByteArrayInputStream bis) throws IOException {
    GZIPInputStream gzipIn = new GZIPInputStream(bis);

    DataInputStream dis = new DataInputStream(gzipIn);

    int dataVersion = dis.readInt();
    Instant timestamp = tsFormatter.parse(dis.readUTF(), Instant::from);

    PropEncodingV1 decoded = new PropEncodingV1(dataVersion, timestamp);

    int items = dis.readInt();
    for (int i = 0; i < items; i++) {
      Map.Entry<String,String> e = readKV(dis);
      decoded.add(e.getKey(), e.getValue());
    }

    return decoded;

  }

  private static void writeKV(final String k, final String v, final DataOutputStream dos) {
    try {
      dos.writeUTF(k);
      dos.writeUTF(v);
    } catch (IOException ex) {
      throw new IllegalStateException(
          String.format("Exception excountered writeing props k:'%s', v:'%s", k, v), ex);
    }
  }

  private static Map.Entry<String,String> readKV(final DataInputStream dis) {
    try {
      String k = dis.readUTF();
      String v = dis.readUTF();
      return new AbstractMap.SimpleEntry<>(k, v);

    } catch (IOException ex) {
      throw new IllegalStateException("Could not read property key value pair", ex);
    }
  }

  private static final DateTimeFormatter tsFormatter =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC));

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
