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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class PropSerdesEncoderFactory implements PropSerdes {

  public static final EncodingOptions V1_COMPRESSED =
      new EncodingOptions(EncodingOptions.ValidVersions.V1_0, true);

  public static final EncodingOptions V1_UNCOMPRESSED =
      new EncodingOptions(EncodingOptions.ValidVersions.V1_0, false);

  private final EncodingOptions encodingOptions;

  public PropSerdesEncoderFactory(final EncodingOptions encodingOptions) {
    this.encodingOptions = encodingOptions;
  }

  public boolean isCompressed() {
    return encodingOptions.isCompressed();
  }

  @Override
  public byte[] encode(VersionedProperties vProps) {

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {

      int nextVersion = Math.max(0, vProps.getDataVersion() + 1);

      VersionInfo versionInfo = new VersionInfo.Builder().withTimestamp(Instant.now())
          .withDataVersion(nextVersion).build();

      encodingOptions.encode(dos);

      versionInfo.encode(dos);

      if (encodingOptions.isCompressed()) {
        writeMapCompressed(bos, vProps.getAllProperties());
      } else {
        writeMap(dos, vProps.getAllProperties());
      }
      dos.flush();

      byte[] bytes = bos.toByteArray();

      // update version info on successful encoding
      vProps.updateVersionInfo(versionInfo);

      return bytes;

    } catch (IOException ex) {
      throw new IllegalStateException("Encountered error serializing properties", ex);
    }
  }

  @Override
  public VersionedProperties decode(final byte[] bytes) {

    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis)) {

      EncodingOptions opts = new EncodingOptions(dis);

      VersionInfo info = readVersionInfo(dis);

      Map<String,String> aMap;
      if (opts.isCompressed()) {
        aMap = readCompressedMap(bis);
      } else {
        aMap = readMap(dis);
      }

      return new VersionedPropertiesImpl(info, aMap);

    } catch (IOException ex) {
      throw new UncheckedIOException("Encountered error deserializing properties", ex);
    }
  }

  private VersionInfo readVersionInfo(DataInputStream dis) throws IOException {
    return new VersionInfo(dis);
  }

  private Map<String,String> readCompressedMap(ByteArrayInputStream bis) throws IOException {

    try (GZIPInputStream gzipIn = new GZIPInputStream(bis);
        DataInputStream dis = new DataInputStream(gzipIn)) {
      return readMap(dis);
    }
  }

  private Map<String,String> readMap(DataInputStream dis) throws IOException {

    Map<String,String> aMap = new HashMap<>();
    int items = dis.readInt();

    for (int i = 0; i < items; i++) {
      Map.Entry<String,String> e = readKV(dis);
      aMap.put(e.getKey(), e.getValue());
    }
    return aMap;
  }

  private void writeMapCompressed(final ByteArrayOutputStream bos, final Map<String,String> aMap)
      throws IOException {

    try (GZIPOutputStream gzipOut = new GZIPOutputStream(bos);
        DataOutputStream dos = new DataOutputStream(gzipOut)) {

      writeMap(dos, aMap);

      gzipOut.flush();
      gzipOut.finish();

    } catch (IOException ex) {
      throw new IOException("Encountered error compressing properties", ex);
    }
  }

  private void writeMap(final DataOutputStream dos, final Map<String,String> aMap)
      throws IOException {

    dos.writeInt(aMap.size());

    aMap.forEach((k, v) -> writeKV(k, v, dos));

    dos.flush();
  }

  private void writeKV(final String k, final String v, final DataOutputStream dos) {
    try {
      dos.writeUTF(k);
      dos.writeUTF(v);
    } catch (IOException ex) {
      throw new UncheckedIOException(
          String.format("Exception encountered writing props k:'%s', v:'%s", k, v), ex);
    }
  }

  private Map.Entry<String,String> readKV(final DataInputStream dis) throws IOException {
    String k = dis.readUTF();
    String v = dis.readUTF();
    return new AbstractMap.SimpleEntry<>(k, v);
  }

  public static class EncodingOptions {

    private final ValidVersions encodingVersion;
    private final boolean compress;

    public EncodingOptions(ValidVersions encodingVersion, final boolean compress) {
      this.encodingVersion = encodingVersion;
      this.compress = compress;
    }

    public EncodingOptions(final DataInputStream dis) throws IOException {
      encodingVersion = ValidVersions.valueOf(dis.readUTF());
      compress = dis.readBoolean();
    }

    public void encode(final DataOutputStream dos) throws IOException {
      dos.writeUTF(encodingVersion.name());
      dos.writeBoolean(compress);
    }

    public ValidVersions getEncodingVersion() {
      return encodingVersion;
    }

    public boolean isCompressed() {
      return compress;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", EncodingOptions.class.getSimpleName() + "[", "]")
          .add("encodingVersion=" + encodingVersion).toString();
    }

    public enum ValidVersions {
      V1_0
    }
  }
}
