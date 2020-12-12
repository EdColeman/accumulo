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

import java.time.Instant;
import java.util.Map;

public interface PropEncoding {

  /**
   * Add a property. If the property already exists it is overwritten.
   *
   * @param key
   *          the name of the property
   * @param value
   *          the value of the property.
   */
  void addProperty(String key, String value);

  /**
   * Get a store property or null if it does not exist.
   *
   * @param key
   *          the name of the property.
   * @return the property value.
   */
  String getProperty(String key);

  /**
   * Delete a property.
   *
   * @param key
   *          the name of the property.
   * @return the previous value if the property was present.
   */
  String removeProperty(String key);

  /**
   * Properties are timestamped when the properties are serialized for storage. This is to allow
   * easy comparison of properties that could have been retrieved at different times.
   *
   * @return the timestamp when the properties were serialized.
   */
  Instant getTimestamp();

  /**
   * Properties are store with a data version ar serialization. This allows for comparison of
   * properties that may have been written and retrieved at different times. Along with the
   * timestamp, this could help with develop deconfliction or merging strategies depended on user
   * needs.
   *
   * @return the data version when the properties were serialized.
   */
  int getDataVersion();

  /**
   * Serialize the version information an the properties.
   *
   * @return an array of bytes for storage.
   */
  byte[] toBytes();

  /**
   * Provide user-friend display string.
   *
   * @param prettyPrint
   *          if true, insert new lines to improve readability.
   * @return a formatted string, with optional new lines.
   */
  String print(boolean prettyPrint);

  /**
   * Get an unmodifiable map with all of the property, values.
   *
   * @return An unmodifiable view of the property key, values.
   */
  Map<String,String> getAllProperties();
}
