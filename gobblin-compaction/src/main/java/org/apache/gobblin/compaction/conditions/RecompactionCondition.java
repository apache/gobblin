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

package org.apache.gobblin.compaction.conditions;
import org.apache.gobblin.compaction.dataset.DatasetHelper;

/**
 * There are different recompaction conditions and their combinations in Gobblin recompaction flow . For example,
 * depending on the number of late records, number of late files, or the late files duration, user may choose
 * different criteria or different combination strategies to decide if a recompaction is mandatory.
 *
 * The interface {@link RecompactionCondition} provides a generic API. This is used when
 * {@link org.apache.gobblin.compaction.mapreduce.MRCompactorJobRunner} attempts to check if a recompaction is necessary. Real
 * examination is delegated to {@link org.apache.gobblin.compaction.dataset.Dataset#checkIfNeedToRecompact(DatasetHelper)},
 * which finally invokes this API.
 */

public interface RecompactionCondition {
  boolean isRecompactionNeeded (DatasetHelper helper);
}
