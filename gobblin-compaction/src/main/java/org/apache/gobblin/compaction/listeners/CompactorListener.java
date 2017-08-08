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

package org.apache.gobblin.compaction.listeners;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.compaction.Compactor;
import org.apache.gobblin.compaction.dataset.Dataset;


/**
 * A listener for a {@link Compactor}.
 */
@Alpha
public interface CompactorListener {

  /**
   * Invoked after the compaction for a {@link Dataset} has been completed.
   *
   * @param dataset the {@link Dataset} whose compaction completed
   */
  public void onDatasetCompactionCompletion(Dataset dataset) throws Exception;
}
