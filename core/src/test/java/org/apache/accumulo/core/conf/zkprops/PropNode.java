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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * A PropNode manages a collection of properties for a single scope as a unit. Properties for a
 * scope are accessed with a Map&lt;String,String&gt; interface. The storage and serialization are
 * managed internally.
 *
 */
public class PropNode {

  public enum Compression {
    NONE, GZIP
  }

  private static final Logger log = LoggerFactory.getLogger(PropNode.class);
  private static Gson gson;

  static {
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(Optional.class, new OptionalSerDes());
    gson = builder.create();
  }

  private PropId id;

  private int dataVersion = 1;
  private Compression compressed = Compression.GZIP;

  private Map<String,String> props = new TreeMap<>();

  private PropNode(final PropId id) {
    this.id = id;
  }

  /** default for serialization */
  private PropNode() {}

  public static class Factory {

    PropId id;
    PropStore store;

    public PropNode.Factory with(Consumer<PropNode.Factory> factory) {
      factory.accept(this);
      return this;
    }

    public PropNode create() {
      return new PropNode(id);
    }
  }

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

    byte[] p = gson.toJson(this, PropNode.class).getBytes(UTF_8);

    byte[] d = compress();

    log.info("Compression stats: before {} - after {} ", p.length, d.length);

    ByteArrayOutputStream bos = new ByteArrayOutputStream(9 + d.length);
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

      PropNode m = PropNode.decompress(bis, len);

      // String s = new String(b.array(),9,len,StandardCharsets.UTF_8);
      // log.debug("s: '{}'", s);
      //
      // Type typeOfHashMap = new TypeToken<Map<String, String>>() { }.getType();
      // Map<String,String> m = gson.fromJson(s, Map.class);

      return m;
    }
  }

  public byte[] compress() throws IOException {

    byte[] data = gson.toJson(this, PropNode.class).getBytes();

    ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
    GZIPOutputStream gzip = new GZIPOutputStream(bos);
    gzip.write(data);
    gzip.close();

    bos.close();

    byte[] compressed = bos.toByteArray();

    log.debug("x:{}", compressed[0]);

    return compressed;
  }

  /**
   * Re-hydrate a PropNode from a gzip'd compressed json string.
   *
   * @param inputStream
   *          an input stream pointing to the compressed json data
   * @param len
   *          the length of the compressed data
   * @return a PropNode
   * @throws IOException
   *           thrown if there is a failure processing the compressed data
   */
  public static PropNode decompress(InputStream inputStream, int len) throws IOException {

    try (GZIPInputStream gis = new GZIPInputStream(inputStream, len)) {
      return gson.fromJson(new InputStreamReader(gis, UTF_8), PropNode.class);
    }
  }

  /**
   * Re-hydrate a PropNode stored compressed in a byte array[]
   *
   * @param compressed
   *          the compressed PropNode json string
   * @return a PropNode
   * @throws IOException
   *           thrown if there is an error processing the compressed data.
   */
  public static PropNode decompress(byte[] compressed) throws IOException {

    log.debug("x:{}", compressed[0]);

    try (ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
        GZIPInputStream gis = new GZIPInputStream(bis)) {
      return gson.fromJson(new InputStreamReader(gis, UTF_8), PropNode.class);
    }
  }

  /**
   * handle java Optional types in json serialization.
   *
   * @param <T>
   *          an instance of an Optional - any type.
   */
  private static class OptionalSerDes<T>
      implements JsonSerializer<Optional<T>>, JsonDeserializer<Optional<T>> {
    @Override
    public Optional<T> deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      final T value =
          context.deserialize(json, ((ParameterizedType) typeOfT).getActualTypeArguments()[0]);
      return Optional.of(value);
    }

    @Override
    public JsonElement serialize(Optional<T> src, Type typeOfSrc,
        JsonSerializationContext context) {
      return context.serialize(src.orElse(null));
    }
  }

  @Override
  public String toString() {
    return "PropNode{dataVersion=" + dataVersion + ", compressed=" + compressed + ", id=" + id
        + ", props=" + props + '}';
  }
}
