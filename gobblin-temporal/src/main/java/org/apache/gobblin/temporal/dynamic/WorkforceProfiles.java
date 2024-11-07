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
import javax.annotation.concurrent.ThreadSafe;

import com.typesafe.config.Config;
import lombok.Getter;


/** A collection of known {@link WorkerProfile}s, also offering name -> profile resolution for {@link ProfileDerivation} */
@ThreadSafe
public class WorkforceProfiles implements Function<String, Optional<WorkerProfile>> {

  /** Indicates `profileName` NOT found */
  public static class UnknownProfileException extends RuntimeException {
    @Getter private final String profileName;

    public UnknownProfileException(String profileName) {
      super("named '" + WorkforceProfiles.renderName(profileName) + "'");
      this.profileName = profileName;
    }
  }


  public static final String BASELINE_NAME = "";
  public static final String BASELINE_NAME_RENDERING = "<<BASELINE>>";

  /** @return the canonical display name for tracing/debugging, with special handling for {@link #BASELINE_NAME} */
  public static String renderName(String name) {
    return name.equals(BASELINE_NAME) ? BASELINE_NAME_RENDERING : name;
  }


  private final ConcurrentHashMap<String, WorkerProfile> profileByName;

  /** restricted-access ctor: instead use {@link #withBaseline(Config)} */
  private WorkforceProfiles() {
    this.profileByName = new ConcurrentHashMap<>();
  }

  /** @return a new instance with `baselineConfig` as the "baseline profile" */
  public static WorkforceProfiles withBaseline(Config baselineConfig) {
    WorkforceProfiles profiles = new WorkforceProfiles();
    profiles.addProfile(new WorkerProfile(BASELINE_NAME, baselineConfig));
    return profiles;
  }

  /** Add a new, previously unknown {@link WorkerProfile} or throw `RuntimeException` on any attempt to add/redefine a previously known profile */
  public void addProfile(WorkerProfile profile) {
    if (profileByName.putIfAbsent(profile.getName(), profile) != null) {
      throw new RuntimeException("profile '" + WorkforceProfiles.renderName(profile.getName()) + "' already exists!");
    }
  }

  /** @return the {@link WorkerProfile} named `profileName`, when it exists */
  @Override
  public Optional<WorkerProfile> apply(String profileName) {
    return Optional.ofNullable(profileByName.get(profileName));
  }

  /**
   * @return the {@link WorkerProfile} named `profileName` or throw {@link UnknownProfileException} when it does not exist
   * @throws UnknownProfileException when `profileName` is unknown
   */
  public WorkerProfile getOrThrow(String profileName) {
    WorkerProfile profile = profileByName.get(profileName);
    if (profile != null) {
      return profile;
    }
    throw new UnknownProfileException(profileName);
  }

  /** @return how many known profiles, including the baseline */
  public int size() {
    return profileByName.size();
  }
}
