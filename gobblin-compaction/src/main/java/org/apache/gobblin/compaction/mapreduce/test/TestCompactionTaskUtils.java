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
package org.apache.gobblin.compaction.mapreduce.test;

import org.apache.hadoop.fs.Path;

import org.apache.gobblin.compaction.dataset.TimeBasedSubDirDatasetsFinder;
import org.apache.gobblin.compaction.mapreduce.MRCompactor;
import org.apache.gobblin.compaction.source.CompactionSource;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblin;

public class TestCompactionTaskUtils {
  public static final String PATH_SEPARATOR = "/";

  public static final String DEFAULT_INPUT_SUBDIR_TYPE = "minutely";
  public static EmbeddedGobblin createEmbeddedGobblinCompactionJob(String name, String basePath) {
    return createEmbeddedGobblinCompactionJob(name, basePath, DEFAULT_INPUT_SUBDIR_TYPE);
  }

  public static EmbeddedGobblin createEmbeddedGobblinCompactionJob(String name, String basePath, String inputSubdirType) {
    String pattern;
    String outputSubdirType;
    if (inputSubdirType.equals(DEFAULT_INPUT_SUBDIR_TYPE)) {
      pattern = new Path(basePath, "*/*/minutely/*/*/*/*").toString();
      outputSubdirType = "hourly";
    } else {
      pattern = new Path(basePath, "*/*/hourly/*/*/*").toString();
      outputSubdirType = "daily";
    }

    return new EmbeddedGobblin(name)
        .setConfiguration(ConfigurationKeys.SOURCE_CLASS_KEY, CompactionSource.class.getName())
        .setConfiguration(ConfigurableGlobDatasetFinder.DATASET_FINDER_PATTERN_KEY, pattern)
        .setConfiguration(MRCompactor.COMPACTION_INPUT_DIR, basePath)
        .setConfiguration(MRCompactor.COMPACTION_INPUT_SUBDIR, inputSubdirType)
        .setConfiguration(MRCompactor.COMPACTION_DEST_DIR, basePath)
        .setConfiguration(MRCompactor.COMPACTION_DEST_SUBDIR, outputSubdirType)
        .setConfiguration(MRCompactor.COMPACTION_TMP_DEST_DIR, "/tmp/compaction/" + name)
        .setConfiguration(TimeBasedSubDirDatasetsFinder.COMPACTION_TIMEBASED_MAX_TIME_AGO, "3000d")
        .setConfiguration(TimeBasedSubDirDatasetsFinder.COMPACTION_TIMEBASED_MIN_TIME_AGO, "1d")
        .setConfiguration(ConfigurationKeys.MAX_TASK_RETRIES_KEY, "0");
  }
}