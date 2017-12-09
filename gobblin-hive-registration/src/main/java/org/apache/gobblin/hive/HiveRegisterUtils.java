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

package org.apache.gobblin.hive;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicy;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase;
import org.apache.gobblin.hive.spec.HiveSpec;


/**
 * Utility class for registering data into Hive.
 */
public class HiveRegisterUtils {

  private HiveRegisterUtils() {
  }

  /**
   * Register the given {@link Path}s.
   *
   * @param paths The {@link Path}s to be registered.
   * @param state A {@link State} which will be used to instantiate a {@link HiveRegister} and a
   * {@link HiveRegistrationPolicy} for registering the given The {@link Path}s.
   */
  public static void register(Iterable<String> paths, State state) throws IOException {
    try (HiveRegister hiveRegister = HiveRegister.get(state)) {
      HiveRegistrationPolicy policy = HiveRegistrationPolicyBase.getPolicy(state);
      for (String path : paths) {
        for (HiveSpec spec : policy.getHiveSpecs(new Path(path))) {
          hiveRegister.register(spec);
        }
      }
    }
  }
}
