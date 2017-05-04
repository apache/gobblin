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
package gobblin.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.DurationFieldType;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.source.extractor.filebased.FileBasedHelperException;
import gobblin.source.extractor.hadoop.HadoopFsHelper;
import gobblin.util.DatePartitionType;

import static gobblin.source.PartitionedFileSourceBase.DATE_PARTITIONED_SOURCE_PARTITION_PATTERN;

/**
 * PartitionRetriever that is optimized for nested directory structures where data is dumped on a regular basis
 * and most data has likely been processed by Gobblin already.
 *
 * For example, if {@link ConfigurationKeys#SOURCE_FILEBASED_DATA_DIRECTORY} is set to /my/data/, then the class assumes
 * folders following the pattern /my/data/daily/[year]/[month]/[day] are present. It will iterate through all the data
 * under these folders starting from the date specified by {@link #DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE} until
 * either {@link #DATE_PARTITIONED_SOURCE_MAX_FILES_PER_JOB} files have been processed, or until there is no more data
 * to process. For example, if {@link #DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE} is set to 2015/01/01, then the job
 * will read from the folder /my/data/daily/2015/01/01/, /my/data/daily/2015/01/02/, /my/data/2015/01/03/ etc.
 *
 */
public class DatePartitionedNestedRetriever implements PartitionAwareFileRetriever {
  private static final Logger LOG = LoggerFactory.getLogger(DatePartitionedNestedRetriever.class);

  private DateTimeFormatter partitionPatternFormatter;
  private DurationFieldType incrementalUnit;
  private String sourcePartitionPrefix;
  private String sourcePartitionSuffix;
  private Path sourceDir;
  private FileSystem fs;
  private HadoopFsHelper helper;
  private final String expectedExtension;

  public DatePartitionedNestedRetriever(String expectedExtension) {
    this.expectedExtension = expectedExtension;
  }

  @Override
  public void init(SourceState state) {
    DateTimeZone.setDefault(DateTimeZone
        .forID(state.getProp(ConfigurationKeys.SOURCE_TIMEZONE, ConfigurationKeys.DEFAULT_SOURCE_TIMEZONE)));

    initDatePartition(state);
    this.sourcePartitionPrefix =
        state.getProp(PartitionedFileSourceBase.DATE_PARTITIONED_SOURCE_PARTITION_PREFIX, StringUtils.EMPTY);

    this.sourcePartitionSuffix =
        state.getProp(PartitionedFileSourceBase.DATE_PARTITIONED_SOURCE_PARTITION_SUFFIX, StringUtils.EMPTY);
    this.sourceDir = new Path(state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY));
    this.helper = new HadoopFsHelper(state);
  }

  @Override
  public List<FileInfo> getFilesToProcess(long minWatermark, int maxFilesToReturn)
      throws IOException {
    DateTime currentDay = new DateTime();
    DateTime lowWaterMarkDate = new DateTime(minWatermark);
    List<FileInfo> filesToProcess = new ArrayList<>();

    try {
      helper.connect();
      this.fs = helper.getFileSystem();
    } catch (FileBasedHelperException e) {
      throw new IOException("Error initializing FileSystem", e);
    }

    for (DateTime date = lowWaterMarkDate; !date.isAfter(currentDay) && filesToProcess.size() < maxFilesToReturn;
        date = date.withFieldAdded(incrementalUnit, 1)) {

      // Constructs the path folder - e.g. /my/data/prefix/2015/01/01/suffix
      Path sourcePath = constructSourcePath(date);

      if (this.fs.exists(sourcePath)) {
        for (FileStatus fileStatus : this.fs.listStatus(sourcePath, getFileFilter())) {
          LOG.info("Will process file " + fileStatus.getPath());
          filesToProcess.add(new FileInfo(fileStatus.getPath().toString(), fileStatus.getLen(), date.getMillis()));
        }
      }
    }

    return filesToProcess;
  }

  @Override
  public long getWatermarkFromString(String lowWaterMark) {
    return this.partitionPatternFormatter.parseMillis(lowWaterMark);
  }

  @Override
  public long getWatermarkIncrementMs() {
    return new DateTime(0).withFieldAdded(this.incrementalUnit, 1).getMillis();
  }

  private void initDatePartition(State state) {
    initDatePartitionFromPattern(state);
    if (this.partitionPatternFormatter == null) {
      initDatePartitionFromGranularity(state);
    }
  }

  private void initDatePartitionFromPattern(State state) {
    String partitionPattern = null;
    try {
      partitionPattern = state.getProp(DATE_PARTITIONED_SOURCE_PARTITION_PATTERN);
      if (partitionPattern != null) {
        this.partitionPatternFormatter =
            DateTimeFormat.forPattern(partitionPattern).withZone(DateTimeZone.getDefault());
        this.incrementalUnit = DatePartitionType.getLowestIntervalUnit(partitionPattern).getDurationType();
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid source partition pattern: " + partitionPattern, e);
    }
  }

  private void initDatePartitionFromGranularity(State state) {
    String granularityProp = state.getProp(PartitionedFileSourceBase.DATE_PARTITIONED_SOURCE_PARTITION_GRANULARITY);
    DatePartitionType partitionType = null;
    if (granularityProp == null) {
      partitionType = PartitionedFileSourceBase.DEFAULT_DATE_PARTITIONED_SOURCE_PARTITION_GRANULARITY;
    } else {
      Optional<DatePartitionType> partitionTypeOpt =
          Enums.getIfPresent(DatePartitionType.class, granularityProp.toUpperCase());
      Preconditions
          .checkState(partitionTypeOpt.isPresent(), "Invalid source partition granularity: " + granularityProp);
      partitionType = partitionTypeOpt.get();
    }
    this.partitionPatternFormatter = DateTimeFormat.forPattern(partitionType.getDateTimePattern());
    this.incrementalUnit = partitionType.getDateTimeFieldType().getDurationType();
  }

  private Path constructSourcePath(DateTime date) {
    StringBuilder pathBuilder = new StringBuilder();

    if (!this.sourcePartitionPrefix.isEmpty()) {
      pathBuilder.append(this.sourcePartitionPrefix);
      pathBuilder.append(Path.SEPARATOR);
    }

    pathBuilder.append(this.partitionPatternFormatter.print(date));

    if (!this.sourcePartitionSuffix.isEmpty()) {
      pathBuilder.append(Path.SEPARATOR);
      pathBuilder.append(this.sourcePartitionSuffix);
    }

    return new Path(this.sourceDir, pathBuilder.toString());
  }

  /**
   * This method is to filter out files that don't need to be processed by extension
   * @return the pathFilter
   */
  private PathFilter getFileFilter() {
    final String extension = (this.expectedExtension.startsWith(".")) ?
        this.expectedExtension :
        "." + this.expectedExtension;

    return new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(extension);
      }
    };
  }
}
