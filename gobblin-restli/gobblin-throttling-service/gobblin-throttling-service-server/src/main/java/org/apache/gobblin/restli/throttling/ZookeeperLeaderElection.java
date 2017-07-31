/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.restli.throttling;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.gobblin.util.SerializationUtils;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link LeaderFinder} using Zookeeper.
 */
@Slf4j
public class ZookeeperLeaderElection<T extends LeaderFinder.Metadata> extends AbstractIdleService implements LeaderFinder<T> {

  private final String leaderElectionNode;
  private final String leaderNode;
  private final T localMetadata;
  private final String zkConnectString;

  private CuratorFramework zooKeeper;
  private String nodeId;
  private boolean isLeader;
  private T leaderMetadata;

  private volatile boolean fatalFailure = false;
  private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  /**
   * @param zkConnectString Zookeeper connect string.
   * @param clusterName Cluster name. Processes in the same cluster are identified by the cluster name.
   * @param localMetadata {@link org.apache.gobblin.restli.throttling.LeaderFinder.Metadata} for the local process.
   * @throws IOException
   */
  public ZookeeperLeaderElection(String zkConnectString, String clusterName, T localMetadata) throws IOException {
    this.zkConnectString = zkConnectString;
    this.localMetadata = localMetadata;
    if (!clusterName.startsWith("/")) {
      clusterName = "/" + clusterName;
    }
    this.leaderElectionNode = clusterName + "/leaderElection";
    this.leaderNode = clusterName + "/leader";
  }

  @Override
  public boolean isLeader() {
    if (this.fatalFailure) {
      throw new IllegalStateException(ZookeeperLeaderElection.class.getSimpleName() + " has failed fatally.");
    }
    ReentrantReadWriteLock.ReadLock lock = this.readWriteLock.readLock();
    lock.lock();
    try {
      return this.isLeader;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public T getLeaderMetadata() {
    if (this.fatalFailure) {
      throw new IllegalStateException(ZookeeperLeaderElection.class.getSimpleName() + " has failed fatally.");
    }
    ReentrantReadWriteLock.ReadLock lock = this.readWriteLock.readLock();
    lock.lock();
    try {
      return this.leaderMetadata;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public T getLocalMetadata() {
    return this.localMetadata;
  }

  @Override
  protected void startUp() throws Exception {
    reset();
  }

  @Override
  protected void shutDown() throws Exception {
    if (this.zooKeeper != null) {
      this.zooKeeper.close();
    }
  }

  private byte[] serializeMetadata(T metadata) throws IOException {
    return SerializationUtils.serializeIntoBytes(metadata);
  }

  private T deserializeMetadata(byte[] bytes) throws IOException, ClassNotFoundException {
    return (T) SerializationUtils.deserializeFromBytes(bytes, Metadata.class);
  }

  private void reset() {
    ReentrantReadWriteLock.WriteLock lock = this.readWriteLock.writeLock();
    lock.lock();
    try {
      if (this.zooKeeper != null) {
        this.zooKeeper.close();
      }

      this.zooKeeper = CuratorFrameworkFactory.builder().retryPolicy(new ExponentialBackoffRetry(100, 3))
          .connectString(this.zkConnectString).build();
      this.zooKeeper.start();
      if (!this.zooKeeper.blockUntilConnected(1, TimeUnit.SECONDS)) {
        throw new RuntimeException("Could not connect to Zookeeper.");
      }
      String nodePath = this.zooKeeper.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
          .forPath(this.leaderElectionNode + "/p_");
      this.nodeId = nodePath.substring(nodePath.lastIndexOf("/") + 1);
      determineLeadership();
    } catch (Throwable exc) {
      throw new RuntimeException(exc);
    } finally {
      lock.unlock();
    }
  }

  private void determineLeadership() {
    ReentrantReadWriteLock.WriteLock lock = this.readWriteLock.writeLock();
    lock.lock();
    try {
      List<String> children = this.zooKeeper.getChildren().forPath(this.leaderElectionNode);
      Collections.sort(children);

      int idx = children.indexOf(this.nodeId);
      if (idx == 0) {
        Stat stat = this.zooKeeper.checkExists().forPath(this.leaderNode);
        if (stat == null) {
          this.zooKeeper.create().forPath(this.leaderNode, serializeMetadata(this.localMetadata));
        } else {
          this.zooKeeper.setData().forPath(this.leaderNode, serializeMetadata(this.localMetadata));
        }
        this.isLeader = true;
      } else {
        this.isLeader = false;
        String watchedNode = this.leaderElectionNode + "/" + children.get(idx - 1);
        this.zooKeeper.checkExists().usingWatcher(new DetermineLeadershipWatcher()).forPath(watchedNode);
      }
      findLeader();
    } catch (KeeperException exc) {
      reset();
    } catch (Throwable exc) {
      log.error("Fatal failure.", exc);
      this.fatalFailure = true;
    } finally {
      lock.unlock();
    }
  }

  private void findLeader() {
    ReentrantReadWriteLock.WriteLock lock = this.readWriteLock.writeLock();
    lock.lock();
    try {
      if (this.zooKeeper.checkExists().usingWatcher(new FindLeaderWatcher()).forPath(this.leaderNode) == null) {
        determineLeadership();
      }
      byte[] leaderData = this.zooKeeper.getData().usingWatcher(new FindLeaderWatcher()).forPath(this.leaderNode);
      this.leaderMetadata = deserializeMetadata(leaderData);
    } catch (KeeperException exc) {
      reset();
    } catch (Throwable exc) {
        log.error("Fatal failure.", exc);
        this.fatalFailure = true;
    } finally {
      lock.unlock();
    }
  }

  public class FindLeaderWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      findLeader();
    }
  }

  public class DetermineLeadershipWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      determineLeadership();
    }
  }
}
