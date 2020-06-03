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
package org.apache.accumulo.core.conf.zkprops;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class PropNode {

  public enum Compression {
    NONE, GZIP
  }

  private static final Logger log = LoggerFactory.getLogger(PropNode.class);
  private static final Gson gson = new Gson();

  private int dataVersion = 1;
  private Compression compressed = Compression.NONE;

  private Map<String,String> props = new TreeMap<>();

  public void setProp(final String name, final String value) {
    props.put(name, value);
  }

  public String toJson() {
    return gson.toJson(this, PropNode.class);
  }

  public static PropNode fromJson(final String payload) {
    return gson.fromJson(payload, PropNode.class);
  }

  public byte[] toByteBuffer() throws IOException {

    byte[] p = gson.toJson(props, Map.class).getBytes(StandardCharsets.UTF_8);

    byte[] d = compress();

    ByteArrayOutputStream bos = new ByteArrayOutputStream(9 + p.length);
    DataOutputStream dos = new DataOutputStream(bos);

    dos.writeInt(dataVersion);
    dos.writeByte(compressed.ordinal());
    dos.writeInt(d.length);
    dos.write(d);

    return bos.toByteArray();
  }

  public static PropNode fromBytes(final byte[] array) throws IOException {

    try (ByteArrayInputStream bis = new ByteArrayInputStream(array);
        DataInputStream dis = new DataInputStream(bis)) {

      PropNode r = new PropNode();

      r.dataVersion = dis.readInt();
      r.compressed = Compression.values()[dis.readByte()];

      int len = dis.readInt();

//      byte[] c = new byte[80];
//      dis.read(c, 0, len);

      Map<String,String> m = PropNode.decompress(bis, len);

      //    String s = new String(b.array(),9,len,StandardCharsets.UTF_8);
      //    log.debug("s: '{}'", s);
      //
      //    Type typeOfHashMap = new TypeToken<Map<String, String>>() { }.getType();
      //    Map<String,String> m = gson.fromJson(s, Map.class);

      r.props = m;

      return r;
    }
  }

  public byte[] compress() throws IOException {

    byte[] data = gson.toJson(props, Map.class).getBytes();

    ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
    GZIPOutputStream gzip = new GZIPOutputStream(bos);
    gzip.write(data);
    gzip.close();

    bos.close();

    byte[] compressed = bos.toByteArray();

    log.debug("x:{}", compressed[0]);

    return compressed;
  }
  public static Map<String,String> decompress(InputStream inputStream, int len) throws IOException {

    try (GZIPInputStream gis = new GZIPInputStream(inputStream, len)) {
      return gson.fromJson(new InputStreamReader(gis, "UTF-8"), Map.class);
    }
  }
  public static Map<String,String> decompress(byte[] compressed) throws IOException {

    log.debug("x:{}", compressed[0]);

    try (ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
        GZIPInputStream gis = new GZIPInputStream(bis)) {
      return gson.fromJson(new InputStreamReader(gis, "UTF-8"), Map.class);
    }
  }

  @Override public String toString() {
    return "PropNode{" + "dataVersion=" + dataVersion + ", compressed=" + compressed + ", props=" + props + '}';
  }
}
