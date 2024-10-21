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

package org.apache.gobblin.temporal.dynamic;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.typesafe.config.Config;


public class WorkforceProfiles implements Function<String, Optional<WorkerProfile>> {
  public static final String BASELINE_NAME = "";
  public static final String BASELINE_NAME_RENDERING = "<<BASELINE>>";

  public static String renderName(String name) {
    return name.equals(BASELINE_NAME) ? BASELINE_NAME_RENDERING : name;
  }


  public static class UnknownProfileException extends RuntimeException {
    public UnknownProfileException(String profileName) {
      super("named '" + WorkforceProfiles.renderName(profileName) + "'");
    }
  }

  private final ConcurrentHashMap<String, WorkerProfile> profileByName;

  private WorkforceProfiles() {
    this.profileByName = new ConcurrentHashMap<>();
  }

  public static WorkforceProfiles withBaseline(Config baselineConfig) {
    WorkforceProfiles profiles = new WorkforceProfiles();
    profiles.addProfile(new WorkerProfile(BASELINE_NAME, baselineConfig));
    return profiles;
  }

  @Override
  public Optional<WorkerProfile> apply(String profileName) {
    return Optional.ofNullable(profileByName.get(profileName));
  }

  public WorkerProfile getOrThrow(String profileName) {
    WorkerProfile profile = profileByName.get(profileName);
    if (profile != null) {
      return profile;
    }
    throw new UnknownProfileException(profileName);
  }

  public void addProfile(WorkerProfile profile) {
    if (profileByName.putIfAbsent(profile.getName(), profile) != null) {
      throw new RuntimeException("profile '" + WorkforceProfiles.renderName(profile.getName()) + "' already exists!");
    }
  }

  public int size() {
    return profileByName.size();
  }
}
