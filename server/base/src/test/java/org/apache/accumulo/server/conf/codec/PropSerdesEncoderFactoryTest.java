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

import static org.apache.accumulo.server.conf.codec.PropSerdesEncoderFactory.V1_COMPRESSED;
import static org.apache.accumulo.server.conf.codec.PropSerdesEncoderFactory.V1_UNCOMPRESSED;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropSerdesEncoderFactoryTest {

  private static final Logger log = LoggerFactory.getLogger(PropSerdesEncoderFactoryTest.class);

  @Test
  public void roundTripUncompressed() {

    VersionInfo versionInfo = new VersionInfo.Builder().build();

    VersionedProperties vProps = new VersionedPropertiesImpl(versionInfo);
    vProps.addProperty("k1", "v1");

    PropSerdes encoder = new PropSerdesEncoderFactory(V1_UNCOMPRESSED);

    byte[] encodedMapBytes = encoder.encode(vProps);

    VersionedProperties decodedProps = encoder.decode(encodedMapBytes);

    log.info("Decoded: {}", decodedProps.getAllProperties());

    assertEquals(vProps.getAllProperties(), decodedProps.getAllProperties());
  }

  @Test
  public void roundTripCompressed() {

    VersionInfo versionInfo = new VersionInfo.Builder().build();

    VersionedProperties vProps = new VersionedPropertiesImpl(versionInfo);
    vProps.addProperty("k1", "v1");

    PropSerdes encoder = new PropSerdesEncoderFactory(V1_COMPRESSED);

    byte[] encodedMapBytes = encoder.encode(vProps);

    VersionedProperties decodedProps = encoder.decode(encodedMapBytes);

    log.info("Decoded: {}", decodedProps.getAllProperties());

    assertEquals(vProps.getAllProperties(), decodedProps.getAllProperties());
  }
}
