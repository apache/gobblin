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

package gobblin.data.management.hive;

import java.io.IOException;
import java.util.List;

import gobblin.annotation.Alpha;
import gobblin.dataset.Dataset;
import gobblin.hive.spec.HiveSpec;


/**
 * A {@link Dataset} that can be registered in Hive.
 */
@Alpha
public interface HiveRegistrableDataset extends Dataset {

  /**
   * Get a list of {@link HiveSpec}s for this dataset, which can be used by {@link gobblin.hive.HiveRegister}
   * to register this dataset in Hive.
   */
  public List<HiveSpec> getHiveSpecs() throws IOException;
}
