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
import java.util.Optional;
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
 * A utility class to handle serialization / deserialization of data. Supports json encoding and
 * optionally compressing the output with Gzip compression.
 */
public class SerdesBase<T> {

  private static final Logger log = LoggerFactory.getLogger(SerdesBase.class);

  static final Gson gson;

  /**
   * defines the max number of bytes that will be encoded / decoded. Defined by zookeeper default
   * jute.maxbuffer of 1M
   */
  private static final int PAYLOAD_LIMIT_MAX_BYTES = 0xfffff;

  static {
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(Optional.class, new OptionalSerDes<>());
    gson = builder.create();
  }

  private final int encodingVersion = 1;
  private JsonSerdes.Compression compressed = JsonSerdes.Compression.GZIP;

  public boolean isCompressionEnabled() {
    return !compressed.equals(JsonSerdes.Compression.NONE);
  }

  /**
   * Enable gzip compression on write.
   */
  public void enableCompression() {
    compressed = JsonSerdes.Compression.GZIP;
  }

  /**
   * Disable compression on write.
   */
  public void disableCompression() {
    compressed = JsonSerdes.Compression.NONE;
  }

  /**
   * Read json encoded PropNode from a stream an return a new instance.
   *
   * @param payload
   *          and input stream of a json encoded prop node.
   * @return a new instance from the json data.
   */
  public T fromJsonStream(final InputStream payload, final Class<T> clazz) {
    return gson.fromJson(new InputStreamReader(payload), clazz);
  }

  /**
   * Re-hydrate a prop node from a byte array
   *
   * @param array
   *          the serialized prop node in an array
   * @return an prop node from the deserialized input.
   * @throws IOException
   *           if am error occurs processing the underlying array.
   */
  public T fromBytes(final byte[] array, final Class<T> clazz) throws IOException {

    try (ByteArrayInputStream bis = new ByteArrayInputStream(array);
        DataInputStream dis = new DataInputStream(bis)) {

      int ver = dis.readInt();
      validateVersion(ver);

      JsonSerdes.Compression compression = JsonSerdes.Compression.values()[dis.readByte()];

      int len = dis.readInt();

      if (compression.equals(JsonSerdes.Compression.GZIP)) {
        return decompress(bis, len, clazz);
      }

      return fromJsonStream(dis, clazz);
    }
  }

  /**
   * Validates that the length is less than the limit. Throws IllegalStateException if the limit is
   * exceeded or prints a warning when size approaches 75% of the limit.
   * <p />
   * This method provides a sanity check so that serialized data that could be from an external
   * source is a reasonable length and that data written does not impact the storage layer.
   * <p />
   * The use case for this serialization is storage in zookeeper - the default jute.buffer max size
   * is used. Increasing the limit may impact zookeeper.
   *
   * @param length
   *          the length of the buffer to be encoded / decoded.
   */
  private void validatePayloadSize(final int length) {

    if (length >= PAYLOAD_LIMIT_MAX_BYTES) {
      throw new IllegalStateException(
          "Encoding serialization " + length + " is greater than limit " + PAYLOAD_LIMIT_MAX_BYTES);
    }

    if (length > 0.75 * PAYLOAD_LIMIT_MAX_BYTES) {
      log.warn(
          "Encoding serialization greater than 75%. Length " + length + " is approaching limit "
              + PAYLOAD_LIMIT_MAX_BYTES + " - compression is enabled: " + isCompressionEnabled());
    }
  }

  private void validateVersion(final int received) {
    if (received != encodingVersion) {
      throw new IllegalStateException("Unsupported data version " + received);
    }
  }

  /**
   * Re-hydrate a PropNode from a gzip'd compressed json string.
   *
   * @param inputStream
   *          an input stream pointing to the compressed json data
   * @param len
   *          the length of the compressed data
   * @param clazz
   *          the return tpe of class
   * @return a decompressed instance of type T
   * @throws IOException
   *           thrown if there is a failure processing the compressed data
   */
  public T decompress(InputStream inputStream, int len, Class<T> clazz) throws IOException {

    try (GZIPInputStream gis = new GZIPInputStream(inputStream, len)) {
      return gson.fromJson(new InputStreamReader(gis, UTF_8), clazz);
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
  public T decompress(byte[] compressed, Class<T> clazz) throws IOException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
        GZIPInputStream gis = new GZIPInputStream(bis)) {
      return gson.fromJson(new InputStreamReader(gis, UTF_8), clazz);
    }
  }

  public String toJson(final T data, Class<T> clazz) {
    return gson.toJson(data, clazz);
  }

  public byte[] toByteBuffer(final T target, Class<T> clazz) throws IOException {

    byte[] bytesOut;
    if (isCompressionEnabled()) {
      bytesOut = compress(target, clazz);
    } else {
      bytesOut = gson.toJson(target, clazz).getBytes(UTF_8);
    }

    validatePayloadSize(bytesOut.length);

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(9 + bytesOut.length);
        DataOutputStream dos = new DataOutputStream(bos)) {
      dos.writeInt(encodingVersion);
      dos.writeByte(compressed.ordinal());
      dos.writeInt(bytesOut.length);
      dos.write(bytesOut);

      return bos.toByteArray();
    } catch (IOException ex) {
      log.debug("Failed to write byte buffer", ex);
      throw ex;
    }
  }

  public byte[] compress(final T target, final Class<T> clazz) throws IOException {

    byte[] bytes = gson.toJson(target, clazz).getBytes();

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(bytes.length);
        GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
      gzip.write(bytes);
      gzip.close();

      bos.close();

      return bos.toByteArray();
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
}
