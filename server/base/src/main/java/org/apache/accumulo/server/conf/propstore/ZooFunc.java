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
package org.apache.accumulo.server.conf.propstore;

import java.util.Objects;
import java.util.StringJoiner;

import org.apache.accumulo.core.data.TableId;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZooFunc {

  private ZooFunc() {
    throw new UnsupportedOperationException("Static utility class, should not be instantiated");
  }

  /**
   * Provide a more friendly toString for zookeeper Stat nodes.
   *
   * @param stat
   *          a zookeeper node stat
   * @return a formatted string.
   */
  public static String prettyStat(final Stat stat) {
    if (Objects.isNull(stat)) {
      return "Stat[null]";
    }

    return new StringJoiner(", ", "Stat[", "]").add("czxid=" + stat.getCzxid())
        .add("mzxid=" + stat.getMzxid()).add("ctime=" + stat.getCtime())
        .add("mtime=" + stat.getMtime()).add("version=" + stat.getVersion())
        .add("cversion=" + stat.getCversion()).add("aversion=" + stat.getAversion())
        .add("ephemeralOwner=" + stat.getEphemeralOwner()).add("dataLength=" + stat.getDataLength())
        .add("numChildren=" + stat.getNumChildren()).add("pzxid=" + stat.getPzxid()).toString();
  }

  public static class PropData {
    private final Stat stat;
    private final ZkMap props;

    public PropData(final Stat stat, final ZkMap props) {
      this.stat = stat;
      this.props = props;
    }

    public Stat getStat() {
      return stat;
    }

    public ZkMap getProps() {
      return props;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", PropData.class.getSimpleName() + "[", "]").add("stat=" + stat)
          .add("props=" + props).toString();
    }
  }

  public static PropData getFromZookeeper(final String rootPath, final TableId tableId,
      final ZooKeeper zoo) {

    Stat stat = new Stat();
    try {

      byte[] data = zoo.getData(rootPath + "/" + tableId, false, stat);

      if (data.length > 0) {
        return new PropData(stat, ZkMap.fromJson(data));
      } else {
        return new PropData(stat, new ZkMap(tableId, stat.getVersion()));
      }
    } catch (KeeperException.NoNodeException ex) {
      return null;
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "invalid return from zookeeper for '" + tableId.canonical() + "'", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  public static boolean waitForZooConnection(ZooKeeper zoo) throws InterruptedException {

    long[] delay = {125, 250, 250, 1_000, 3_000, 5_000};

    boolean connected = false;

    int attempt = 0;
    long totalDelay = 0;

    while (!connected) {
      ZooKeeper.States zooState = zoo.getState();
      if (zooState == ZooKeeper.States.CONNECTED) {
        connected = true;
      } else {
        if (attempt >= delay.length) {
          throw new IllegalStateException("could not connect to zookeeper after " + delay.length
              + " attempts. Time waiting (" + totalDelay + ") ms");
        }
        totalDelay += delay[attempt];
        Thread.sleep(delay[attempt++]);
      }
    }
    return connected;
  }

}
