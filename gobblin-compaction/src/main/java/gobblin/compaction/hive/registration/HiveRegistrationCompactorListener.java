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

package gobblin.compaction.hive.registration;

import java.io.IOException;
import java.util.Properties;

import gobblin.compaction.listeners.CompactorListener;
import gobblin.compaction.dataset.Dataset;
import gobblin.configuration.State;
import gobblin.hive.HiveRegister;
import gobblin.hive.policy.HiveRegistrationPolicy;
import gobblin.hive.policy.HiveRegistrationPolicyBase;
import gobblin.hive.spec.HiveSpec;


public class HiveRegistrationCompactorListener implements CompactorListener {

  private final HiveRegister hiveRegister;
  private final HiveRegistrationPolicy hiveRegistrationPolicy;

  public HiveRegistrationCompactorListener(Properties properties) throws IOException {
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
