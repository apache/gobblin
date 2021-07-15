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
package org.apache.gobblin.service;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Splitter;
import com.google.gson.JsonObject;
import com.typesafe.config.Config;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.util.filesystem.PathAlterationObserver;


/**
 * Reads and updates from a JSON where keys denote group names
 * and values denote a list of group members
 */
@Alias("local")
@Singleton
public class LocalGroupOwnershipService extends GroupOwnershipService {
  public static final String GROUP_MEMBER_LIST = "groupOwnershipService.groupMembers.path";
  LocalGroupOwnershipPathAlterationListener listener;
  PathAlterationObserver observer;

  @Inject
  public LocalGroupOwnershipService(Config config) {
    Path groupOwnershipFilePath = new Path(config.getString(GROUP_MEMBER_LIST));
    try {
      observer = new PathAlterationObserver(groupOwnershipFilePath.getParent());
      this.listener = new LocalGroupOwnershipPathAlterationListener(groupOwnershipFilePath);
      observer.addListener(this.listener);
    } catch (IOException e) {
      throw new RuntimeException("Could not get initialize PathAlterationObserver at %" + groupOwnershipFilePath.toString(), e);
    }
  }


  @Override
  public boolean isMemberOfGroup(List<ServiceRequester> serviceRequesters, String group) {
    // ensure that the group ownership file is up to date
    try {
      this.observer.checkAndNotify();
    } catch (IOException e) {
      throw new RuntimeException("Group Ownership observer could not check for file changes", e);
    }

    JsonObject groupOwnerships = this.listener.getGroupOwnerships();
    if (groupOwnerships.has(group)) {
      List<String> groupMembers = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(
          groupOwnerships.get(group).getAsString());
      for (ServiceRequester requester: serviceRequesters) {
        if (groupMembers.contains(requester.getName())) {
          return true;
        }
      }
    }
    return false;
  }


}
