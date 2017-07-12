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

package gobblin.compaction.action;
import gobblin.dataset.Dataset;
import gobblin.metrics.event.EventSubmitter;

import java.io.IOException;


/**
 * An interface which represents an action that is invoked after a compaction job is finished.
 */
public interface CompactionCompleteAction<D extends Dataset> {

  void onCompactionJobComplete(D dataset) throws IOException;

  default void addEventSubmitter(EventSubmitter submitter) {
    throw new UnsupportedOperationException("Please add an EventSubmitter");
  }
}
