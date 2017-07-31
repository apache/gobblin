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

package org.apache.gobblin.compaction.dataset;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.joda.time.DateTimeZone;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.base.Optional;

import org.apache.gobblin.compaction.conditions.RecompactionCondition;
import org.apache.gobblin.compaction.conditions.RecompactionConditionFactory;
import org.apache.gobblin.compaction.mapreduce.MRCompactor;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.FileListUtils;
import org.apache.gobblin.util.RecordCountProvider;
import org.apache.gobblin.util.recordcount.LateFileRecordCountProvider;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A class {@link DatasetHelper} which provides runtime metrics and other helper functions for a given dataset.
 *
 * The class also contains different recompaction conditions {@link RecompactionCondition}, which indicates if a
 * recompaction is needed. These conditions will be examined by {@link org.apache.gobblin.compaction.mapreduce.MRCompactorJobRunner}
 * after late data was found and copied from inputDir to outputLateDir.
 */

public class DatasetHelper {
  private final FileSystem fs;
  private final Dataset dataset;
  private final RecordCountProvider inputRecordCountProvider;
  private final RecordCountProvider outputRecordCountProvider;
  private final LateFileRecordCountProvider lateInputRecordCountProvider;
  private final LateFileRecordCountProvider lateOutputRecordCountProvider;
  private final RecompactionCondition condition;
  private final Collection<String> extensions;

  private static final Logger logger = LoggerFactory.getLogger(DatasetHelper.class);

  public DatasetHelper(Dataset dataset, FileSystem fs, Collection<String> extensions) {
    this.extensions = extensions;
    this.fs = fs;
    this.dataset = dataset;
    this.condition = createRecompactionCondition();

    try {
      this.inputRecordCountProvider = (RecordCountProvider) Class
          .forName(this.dataset.jobProps().getProp(MRCompactor.COMPACTION_INPUT_RECORD_COUNT_PROVIDER,
              MRCompactor.DEFAULT_COMPACTION_INPUT_RECORD_COUNT_PROVIDER))
          .newInstance();
      this.outputRecordCountProvider = (RecordCountProvider) Class
          .forName(this.dataset.jobProps().getProp(MRCompactor.COMPACTION_OUTPUT_RECORD_COUNT_PROVIDER,
              MRCompactor.DEFAULT_COMPACTION_OUTPUT_RECORD_COUNT_PROVIDER))
          .newInstance();
      this.lateInputRecordCountProvider = new LateFileRecordCountProvider(this.inputRecordCountProvider);
      this.lateOutputRecordCountProvider = new LateFileRecordCountProvider(this.outputRecordCountProvider);
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate RecordCountProvider", e);
    }
  }

  public Dataset getDataset () {
    return dataset;
  }

  private RecompactionCondition createRecompactionCondition () {
    ClassAliasResolver<RecompactionConditionFactory> conditionClassAliasResolver = new ClassAliasResolver<>(RecompactionConditionFactory.class);
    String factoryName = this.dataset.jobProps().getProp(MRCompactor.COMPACTION_RECOMPACT_CONDITION,
        MRCompactor.DEFAULT_COMPACTION_RECOMPACT_CONDITION);
    try {
      RecompactionConditionFactory factory = GobblinConstructorUtils.invokeFirstConstructor(
          conditionClassAliasResolver.resolveClass(factoryName), ImmutableList.of());
      return factory.createRecompactionCondition(dataset);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static List<Path> getApplicableFilePaths (FileSystem fs, Path dataDir, final Collection<String> extensions) throws IOException {
    if (!fs.exists(dataDir)) {
      return Lists.newArrayList();
    }
    List<Path> paths = Lists.newArrayList();
    for (FileStatus fileStatus : FileListUtils.listFilesRecursively(fs, dataDir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        for (String validExtention : extensions) {
          if (path.getName().endsWith(validExtention)) {
            return true;
          }
        }
        return false;
      }
    })) {
      paths.add(fileStatus.getPath());
    }
    return paths;
  }

  public List<Path> getApplicableFilePaths (Path dataDir) throws IOException {
    return getApplicableFilePaths(fs, dataDir, Lists.newArrayList("avro"));
  }

  public Optional<DateTime> getEarliestLateFileModificationTime() {
    DateTimeZone timeZone = DateTimeZone
        .forID(this.dataset.jobProps().getProp(MRCompactor.COMPACTION_TIMEZONE, MRCompactor.DEFAULT_COMPACTION_TIMEZONE));
    try {
      long maxTimestamp = Long.MIN_VALUE;
      for (FileStatus status : FileListUtils.listFilesRecursively(this.fs, this.dataset.outputLatePath())) {
        maxTimestamp = Math.max(maxTimestamp, status.getModificationTime());
      }
      return maxTimestamp == Long.MIN_VALUE ? Optional.<DateTime>absent():Optional.of(new DateTime(maxTimestamp, timeZone));
    } catch (Exception e) {
      logger.error("Failed to get earliest late file modification time");
      return Optional.absent();
    }
  }

  public DateTime getCurrentTime() {
    DateTimeZone timeZone = DateTimeZone
        .forID(this.dataset.jobProps().getProp(MRCompactor.COMPACTION_TIMEZONE, MRCompactor.DEFAULT_COMPACTION_TIMEZONE));
    DateTime currentTime = new DateTime(timeZone);
    return currentTime;
  }

  public long getLateOutputRecordCount() {
    long lateOutputRecordCount = 0l;
    try {
      Path outputLatePath = dataset.outputLatePath();
      if (this.fs.exists(outputLatePath)) {
        lateOutputRecordCount = this.lateOutputRecordCountProvider
            .getRecordCount(this.getApplicableFilePaths(dataset.outputLatePath()));
      }
    } catch (Exception e) {
      logger.error("Failed to get late record count:" + e, e);
    }
    return lateOutputRecordCount;
  }

  public long getOutputRecordCount() {
    long outputRecordCount = 01;
    try {
      outputRecordCount = this.outputRecordCountProvider.
          getRecordCount(this.getApplicableFilePaths(dataset.outputPath()));
      return outputRecordCount;
    } catch (Exception e) {
      logger.error("Failed to submit late event count:" + e, e);
    }
    return outputRecordCount;
  }

  protected RecompactionCondition getCondition() {
    return condition;
  }

  public long getLateOutputFileCount() {
      long lateOutputFileCount = 0l;
      try {
        Path outputLatePath = dataset.outputLatePath();
        if (this.fs.exists(outputLatePath)) {
          lateOutputFileCount = getApplicableFilePaths(dataset.outputLatePath()).size();
          logger.info("LateOutput File Count is : " + lateOutputFileCount + " at " + outputLatePath.toString());
        }
      } catch (Exception e) {
        logger.error("Failed to get late file count from :" + e, e);
      }
      return lateOutputFileCount;
  }
}

