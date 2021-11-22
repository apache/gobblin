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

package org.apache.gobblin.cluster;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link ZKHelixManager} which keeps a reference count of users.
 * Every user should call connect and disconnect to increase and decrease the count.
 * Calls to connect and disconnect to the underlying ZKHelixManager are made only for the first and last usage respectively.
 */
@Slf4j
public class GobblinReferenceCountingZkHelixManager extends ZKHelixManager {
  private final AtomicInteger usageCount = new AtomicInteger(0);

  public GobblinReferenceCountingZkHelixManager(String clusterName, String instanceName, InstanceType instanceType, String zkAddress) {
    super(clusterName, instanceName, instanceType, zkAddress);
  }

  @Override
  public void connect() throws Exception {
    if (usageCount.incrementAndGet() == 1) {
      super.connect();
    }
  }

  @Override
  public void disconnect() {
    if (usageCount.decrementAndGet() <= 0) {
      super.disconnect();
    }
  }
}
