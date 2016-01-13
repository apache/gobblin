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

package gobblin.data.management.retention.dataset;

import gobblin.data.management.dataset.Dataset;

import java.io.IOException;


/**
 * An abstraction for a set of files where a simple {@link gobblin.data.management.retention.policy.RetentionPolicy}
 * can be applied.
 */
public interface CleanableDataset extends Dataset {

  /**
   * Cleans the {@link CleanableDataset}. In general, this means to apply a
   * {@link gobblin.data.management.retention.policy.RetentionPolicy} and delete files and directories that need deleting.
   * @throws IOException
   */
  public void clean() throws IOException;

}
