/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.fate.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Testable;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A thin wrapper around Zookeeper for debugging.
 */
public class DistConsensus extends ZooKeeper {

  private static final Logger log = LoggerFactory.getLogger(DistConsensus.class);

  private final AtomicInteger constructed = new AtomicInteger(0);

  public DistConsensus(String connectString, int sessionTimeout, Watcher watcher)
      throws IOException {
    super(connectString, sessionTimeout, watcher);
    constructed.incrementAndGet();
  }

  public DistConsensus(String connectString, int sessionTimeout, Watcher watcher,
      boolean canBeReadOnly) throws IOException {
    super(connectString, sessionTimeout, watcher, canBeReadOnly);
    constructed.incrementAndGet();
  }

  public DistConsensus(String connectString, int sessionTimeout, Watcher watcher, long sessionId,
      byte[] sessionPasswd) throws IOException {
    super(connectString, sessionTimeout, watcher, sessionId, sessionPasswd);
    constructed.incrementAndGet();
  }

  public DistConsensus(String connectString, int sessionTimeout, Watcher watcher, long sessionId,
      byte[] sessionPasswd, boolean canBeReadOnly) throws IOException {
    super(connectString, sessionTimeout, watcher, sessionId, sessionPasswd, canBeReadOnly);
    constructed.incrementAndGet();
  }

  public void logMetrics() {
    log.debug("{}", new StringJoiner(", ", DistConsensus.class.getSimpleName() + "[", "]")
        .add("constructed=" + constructed.get()).toString());
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", DistConsensus.class.getSimpleName() + "[", "]")
        .add("constructed=" + constructed).add(super.toString()).toString();
  }

  @Override
  public ZooKeeperSaslClient getSaslClient() {
    return super.getSaslClient();
  }

  @Override
  public Testable getTestable() {
    return super.getTestable();
  }

  @Override
  public long getSessionId() {
    return super.getSessionId();
  }

  @Override
  public byte[] getSessionPasswd() {
    return super.getSessionPasswd();
  }

  @Override
  public int getSessionTimeout() {
    return super.getSessionTimeout();
  }

  @Override
  public void addAuthInfo(String scheme, byte[] auth) {
    super.addAuthInfo(scheme, auth);
  }

  @Override
  public synchronized void register(Watcher watcher) {
    super.register(watcher);
    log.debug("Caller {} registered watcher {}", org.slf4j.helpers.Util.getCallingClass(), watcher);
  }

  @Override
  public synchronized void close() throws InterruptedException {
    super.close();
  }

  @Override
  public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
      throws KeeperException, InterruptedException {
    log.debug("Caller {} created path {}", org.slf4j.helpers.Util.getCallingClass(), path);
    return super.create(path, data, acl, createMode);
  }

  @Override
  public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode,
      AsyncCallback.StringCallback cb, Object ctx) {
    log.debug("Caller {} created (async) path {}", org.slf4j.helpers.Util.getCallingClass(), path);
    super.create(path, data, acl, createMode, cb, ctx);
  }

  @Override
  public void delete(String path, int version) throws InterruptedException, KeeperException {
    log.debug("Caller {} deleted path {}", org.slf4j.helpers.Util.getCallingClass(), path);
    super.delete(path, version);
  }

  @Override
  public List<OpResult> multi(Iterable<Op> ops) throws InterruptedException, KeeperException {
    return super.multi(ops);
  }

  @Override
  public void multi(Iterable<Op> ops, AsyncCallback.MultiCallback cb, Object ctx) {
    super.multi(ops, cb, ctx);
  }

  @Override
  protected void multiInternal(MultiTransactionRecord request, AsyncCallback.MultiCallback cb,
      Object ctx) {
    super.multiInternal(request, cb, ctx);
  }

  @Override
  protected List<OpResult> multiInternal(MultiTransactionRecord request)
      throws InterruptedException, KeeperException {
    return super.multiInternal(request);
  }

  @Override
  public Transaction transaction() {
    return super.transaction();
  }

  @Override
  public void delete(String path, int version, AsyncCallback.VoidCallback cb, Object ctx) {
    log.debug("Caller {} deleted (async) path {}", org.slf4j.helpers.Util.getCallingClass(), path);
    super.delete(path, version, cb, ctx);
  }

  @Override
  public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
    log.debug("Caller {} exists path {}, setting watcher {}",
        org.slf4j.helpers.Util.getCallingClass(), path, watcher);
    return super.exists(path, watcher);
  }

  @Override
  public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
    log.debug("Caller {} exists path {}, watch {}", org.slf4j.helpers.Util.getCallingClass(), path,
        watch);
    return super.exists(path, watch);
  }

  @Override
  public void exists(String path, Watcher watcher, AsyncCallback.StatCallback cb, Object ctx) {
    log.debug("Caller {} exists (async) path {}, setting watcher {}",
        org.slf4j.helpers.Util.getCallingClass(), path, watcher);
    super.exists(path, watcher, cb, ctx);
  }

  @Override
  public void exists(String path, boolean watch, AsyncCallback.StatCallback cb, Object ctx) {
    log.debug("Caller {} exists (async) path {}, watch {}",
        org.slf4j.helpers.Util.getCallingClass(), path, watch);
    super.exists(path, watch, cb, ctx);
  }

  @Override
  public byte[] getData(String path, Watcher watcher, Stat stat)
      throws KeeperException, InterruptedException {
    log.debug("Caller {} getData path {}, setting watcher {}",
        org.slf4j.helpers.Util.getCallingClass(), path, watcher);
    return super.getData(path, watcher, stat);
  }

  @Override
  public byte[] getData(String path, boolean watch, Stat stat)
      throws KeeperException, InterruptedException {
    log.debug("Caller {} getData path {}, watch {}", org.slf4j.helpers.Util.getCallingClass(), path,
        watch);
    return super.getData(path, watch, stat);
  }

  @Override
  public void getData(String path, Watcher watcher, AsyncCallback.DataCallback cb, Object ctx) {
    log.debug("Caller {} getData (async) path {}, watcher {}",
        org.slf4j.helpers.Util.getCallingClass(), path, watcher);
    super.getData(path, watcher, cb, ctx);
  }

  @Override
  public void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx) {
    log.debug("Caller {} getData (async) path {}, watch {}",
        org.slf4j.helpers.Util.getCallingClass(), path, watch);
    super.getData(path, watch, cb, ctx);
  }

  @Override
  public Stat setData(String path, byte[] data, int version)
      throws KeeperException, InterruptedException {
    log.debug("Caller {} setData path {}", org.slf4j.helpers.Util.getCallingClass(), path);
    return super.setData(path, data, version);
  }

  @Override
  public void setData(String path, byte[] data, int version, AsyncCallback.StatCallback cb,
      Object ctx) {
    log.debug("Caller {} setData (async) path {}", org.slf4j.helpers.Util.getCallingClass(), path);
    super.setData(path, data, version, cb, ctx);
  }

  @Override
  public List<ACL> getACL(String path, Stat stat) throws KeeperException, InterruptedException {
    return super.getACL(path, stat);
  }

  @Override
  public void getACL(String path, Stat stat, AsyncCallback.ACLCallback cb, Object ctx) {
    super.getACL(path, stat, cb, ctx);
  }

  @Override
  public Stat setACL(String path, List<ACL> acl, int aclVersion)
      throws KeeperException, InterruptedException {
    return super.setACL(path, acl, aclVersion);
  }

  @Override
  public void setACL(String path, List<ACL> acl, int version, AsyncCallback.StatCallback cb,
      Object ctx) {
    super.setACL(path, acl, version, cb, ctx);
  }

  @Override
  public List<String> getChildren(String path, Watcher watcher)
      throws KeeperException, InterruptedException {
    log.debug("Caller {} getChildren path {}, watcher {}", org.slf4j.helpers.Util.getCallingClass(),
        path, watcher);
    return super.getChildren(path, watcher);
  }

  @Override
  public List<String> getChildren(String path, boolean watch)
      throws KeeperException, InterruptedException {
    log.debug("Caller {} getChildren path {}, watch {}", org.slf4j.helpers.Util.getCallingClass(),
        path, watch);
    return super.getChildren(path, watch);
  }

  @Override
  public void getChildren(String path, Watcher watcher, AsyncCallback.ChildrenCallback cb,
      Object ctx) {
    log.debug("Caller {} getChildren (async) path {}, watcher {}",
        org.slf4j.helpers.Util.getCallingClass(), path, watcher);
    super.getChildren(path, watcher, cb, ctx);
  }

  @Override
  public void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb,
      Object ctx) {
    log.debug("Caller {} getChildren (async) path {}, watch {}",
        org.slf4j.helpers.Util.getCallingClass(), path, watch);
    super.getChildren(path, watch, cb, ctx);
  }

  @Override
  public List<String> getChildren(String path, Watcher watcher, Stat stat)
      throws KeeperException, InterruptedException {
    log.debug("Caller {} getChildren path {}, watcher {}", org.slf4j.helpers.Util.getCallingClass(),
        path, watcher);
    return super.getChildren(path, watcher, stat);
  }

  @Override
  public List<String> getChildren(String path, boolean watch, Stat stat)
      throws KeeperException, InterruptedException {
    log.debug("Caller {} getChildren path {}, watch {}", org.slf4j.helpers.Util.getCallingClass(),
        path, watch);
    return super.getChildren(path, watch, stat);
  }

  @Override
  public void getChildren(String path, Watcher watcher, AsyncCallback.Children2Callback cb,
      Object ctx) {
    log.debug("Caller {} getChildren (async) path {}, watcher {}",
        org.slf4j.helpers.Util.getCallingClass(), path, watcher);
    super.getChildren(path, watcher, cb, ctx);
  }

  @Override
  public void getChildren(String path, boolean watch, AsyncCallback.Children2Callback cb,
      Object ctx) {
    log.debug("Caller {} getChildren (async) path {}, watch {}",
        org.slf4j.helpers.Util.getCallingClass(), path, watch);
    super.getChildren(path, watch, cb, ctx);
  }

  @Override
  public void sync(String path, AsyncCallback.VoidCallback cb, Object ctx) {
    super.sync(path, cb, ctx);
  }

  @Override
  public States getState() {
    return super.getState();
  }

}
