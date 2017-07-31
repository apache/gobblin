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

package org.apache.gobblin.compaction.hive.registration;

import java.util.Properties;

import org.apache.gobblin.compaction.listeners.CompactorListener;
import org.apache.gobblin.compaction.dataset.Dataset;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HiveRegister;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicy;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase;
import org.apache.gobblin.hive.spec.HiveSpec;


public class HiveRegistrationCompactorListener implements CompactorListener {

  private final HiveRegister hiveRegister;
  private final HiveRegistrationPolicy hiveRegistrationPolicy;

  public HiveRegistrationCompactorListener(Properties properties) {
    State state = new State(properties);
    this.hiveRegister = HiveRegister.get(state);
    this.hiveRegistrationPolicy = HiveRegistrationPolicyBase.getPolicy(state);
  }

  @Override
  public void onDatasetCompactionCompletion(Dataset dataset) throws Exception {
    for (HiveSpec spec : this.hiveRegistrationPolicy.getHiveSpecs(dataset.outputPath())) {
      this.hiveRegister.register(spec);
    }
  }
}
