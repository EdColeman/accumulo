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

import java.util.Comparator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZkPropPath implements Comparable<ZkPropPath>{

  // the general form is /accumulo/[instance-id][base][id]
  private static final Pattern pathPattern = Pattern.compile("^/(?<root>\\w*)/(?<instanceId>\\w*)(?<base>/.*)*/(?<id>\\w*)$");

  private final static String root = "/accumulo";
  private final String instance;
  private final String base;
  private final String id;

  private final String canonical;

  public ZkPropPath(final String instance, final String base, final String id){
    this.instance = instance;
    this.base = base;
    this.id = id;

    this.canonical = root + "/" + instance + "/" + base + "/" + id;
  }

  public ZkPropPath(final String path){
    Matcher m = pathPattern.matcher(path);
    if(!m.matches() || m.groupCount() != 4){
      throw new IllegalArgumentException("Invalid zookeepr path provided '" + path
          + "' expected form /accumulo/instance/[path]/id");
    }
    canonical = path;

    instance = m.group("instanceId");
    base = m.group("base");
    id = m.group("id");
  }

  public String getCanonical(){
    return canonical;
  }

  public static String getRoot() {
    return root;
  }

  public String getInstance() {
    return instance;
  }

  public String getBase() {
    return base;
  }

  public String getId() {
    return id;
  }

  @Override public String toString() {
    return "ZkPropPath{" + "canonical='" + canonical + '\'' + '}';
  }

  private static Comparator<ZkPropPath> compareIdThenBase =
      Comparator.comparing(ZkPropPath::getId).thenComparing(ZkPropPath::getBase)
          .thenComparing(ZkPropPath::getInstance);

  @Override public int compareTo(ZkPropPath other) {
    return compareIdThenBase.compare(this, other);
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    ZkPropPath that = (ZkPropPath) o;
    return base.equals(that.base) && id.equals(that.id) && instance.equals(that.instance);
  }

  @Override public int hashCode() {
    return Objects.hash(id, base, instance);
  }
}
