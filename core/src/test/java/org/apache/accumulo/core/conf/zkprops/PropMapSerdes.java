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

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class to handle serialization / deserialization of PropMap. Supports json encoding an
 * optionally compressing the output with Gzip compression.
 */
public class PropMapSerdes extends SerdesBase<PropMap> implements JsonSerdes<PropMap> {

  // TODO - replace with all static methods?
  public PropMapSerdes() {}

  private static final Logger log = LoggerFactory.getLogger(PropMapSerdes.class);

  /**
   * Read son encoded string and return a new instance.
   *
   * @param payload
   *          a json encoded PropNode string.
   * @return a new instance.
   */
  public PropMap fromJson(final String payload) {
    return fromJson(payload, PropMap.class);
  }

  /**
   * Read json encoded PropNode from a stream an return a new instance.
   *
   * @param payload
   *          and input stream of a json encoded prop node.
   * @return a new instance from the json data.
   */
  public PropMap fromJson(final InputStream payload) {
    return fromJson(payload, PropMap.class);
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
  public PropMap fromBytes(final byte[] array) throws IOException {
    return fromBytes(array, PropMap.class);
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
  public PropMap decompress(InputStream inputStream, int len) throws IOException {
    return decompress(inputStream, len, PropMap.class);
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
  public PropMap decompress(byte[] compressed) throws IOException {
    return decompress(compressed, PropMap.class);
  }

  public String toJson(final PropNode node) {
    return gson.toJson(node, PropNode.class);
  }

  public byte[] toByteBuffer(final PropMap target) throws IOException {
    return toByteBuffer(target, PropMap.class);
  }

  public byte[] compress(final PropMap target) throws IOException {
    return compress(target, PropMap.class);
  }

}
