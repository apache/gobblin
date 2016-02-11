/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.hive;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import gobblin.configuration.State;
import gobblin.hive.policy.HiveRegistrationPolicy;
import gobblin.hive.policy.HiveRegistrationPolicyBase;


/**
 * Utility class for registering data into Hive.
 */
public class HiveRegisterUtils {

  /**
   * Register the given {@link Path}s.
   *
   * @param paths The {@link Path}s to be registered.
   * @param state A {@link State} which will be used to instantiate a {@link HiveRegister} and a
   * {@link HiveRegistrationPolicy} for registering the given The {@link Path}s.
   */
  public static void register(Iterable<String> paths, State state) throws IOException {
    try (HiveRegister hiveRegister = new HiveRegister(state)) {
      HiveRegistrationPolicy policy = HiveRegistrationPolicyBase.getPolicy(state);
      for (String path : paths) {
        hiveRegister.register(policy.getHiveSpec(new Path(path)));
      }
    }
  }
}
