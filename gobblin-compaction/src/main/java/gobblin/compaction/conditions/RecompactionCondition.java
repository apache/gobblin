/*
 * Copyright (C) 2016-2018 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction.conditions;
import gobblin.compaction.dataset.DatasetHelper;

/**
 * There are different recompaction conditions and their combinations in Gobblin recompaction flow . For example,
 * based on the number of late records, number of late files, or the late files existence duration, user can choose
 * different criteria or combine all of them to decide if it is the right time to do a recompaction.
 *
 * The interface {@link RecompactionCondition} provides a generic API. This is used when
 * {@link gobblin.compaction.mapreduce.MRCompactorJobRunner} try to decide if a recompaction is necessary and delegate
 * the real examination to {@link gobblin.compaction.dataset.Dataset}, which finally invokes this API.
 */

public interface RecompactionCondition {
  boolean isRecompactionNeeded (DatasetHelper metric);
}
