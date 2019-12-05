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
package org.apache.accumulo.server.conf.propstore.proto2.cache;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.server.conf.propstore.ZooFunc.prettyStat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.conf.propstore.dummy.NodeData;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

public class PropMap {

  private static final Logger log = LoggerFactory.getLogger(PropMap.class);

  private final TableId tableId;
  private Stat stat;

  private Map<Property,String> propMap = new HashMap<>();

  public PropMap(final TableId tableId, final Stat stat) {
    this.tableId = tableId;
    this.stat = stat;
  }

  public static PropMap fromNodeData(final TableId tableId, final NodeData nodeData) {

    if (nodeData.isEmpty()) {
      return new PropMap(tableId, nodeData.getStat());
    }

    PropMap props = PropMap.fromJson(nodeData.getData());
    props.setDataVersion(nodeData.getStat());

    return props;
  }

  public TableId getTableId() {
    return tableId;
  }

  public Stat getStat() {
    return stat;
  }

  public void set(final Property p, String v) {
    propMap.put(p, v);
  }

  /**
   * Get the property value from the map - returning null if absent. If the default value was
   * returned, it would make determining proper level in the heiarchy where the value is set. Null
   * is returned so that the property heiarchy can determine if the property was overridden here or
   * if the default value should be ultimately used.
   *
   * @param p
   *          the property
   * @return an optional containing either the property value or null.
   */
  public Optional<String> get(final Property p) {
    return Optional.ofNullable(propMap.get(p));
  }

  @Override
  public String toString() {
    return "ZkMap{" + "tableId=" + tableId + ", dataVersion=" + prettyStat(stat) + ", mapRef="
        + propMap + '}';
  }

  public byte[] toJson() {
    SerDes serDes = new SerDes(this);
    return serDes.toJson();
  }

  public static PropMap fromJson(byte[] data) {
    return SerDes.fromJson(data);
  }

  public void setDataVersion(final Stat stat) {
    this.stat = stat;
  }

  private static class SerDes {

    // place holder for version migration on upgrade
    @SuppressWarnings("unused")
    private final String serialVersion = "0.1.0";

    private final String tableId;
    private final Map<Property,String> props;

    public SerDes(final PropMap zkMap) {
      this.tableId = zkMap.tableId.canonical();
      this.props = zkMap.propMap;
    }

    private static final Type type = new TypeToken<Map<Property,String>>() {}.getType();

    private static final JsonDeserializer<Map<Property,String>> propDes = new JsonDeserializer<>() {

      @Override
      public Map<Property,String> deserialize(JsonElement jsonElement, Type type,
          JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        Set<Map.Entry<String,JsonElement>> entrySet = jsonObject.entrySet();

        Map<Property,String> r = new HashMap<>();

        for (Map.Entry<String,JsonElement> e : entrySet) {
          r.put(Property.getPropertyByKey(e.getKey()), e.getValue().getAsString());
        }
        return r;
      }
    };

    private static final Gson gson = new GsonBuilder().registerTypeAdapter(type, propDes).create();

    public byte[] toJson() {
      return gson.toJson(this).getBytes(UTF_8);
    }

    public static PropMap fromJson(final byte[] bytes) {

      try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
          InputStreamReader reader = new InputStreamReader(bis)) {
        SerDes sd = gson.fromJson(reader, SerDes.class);

        PropMap zm = new PropMap(TableId.of(sd.tableId), new Stat());
        zm.propMap = sd.props;

        return zm;
      } catch (IOException ex) {
        log.trace("Failed to deserialize {}", new String(bytes, UTF_8));
        throw new IllegalStateException("Failed to deserialize properties", ex);
      }
    }
  }

}
