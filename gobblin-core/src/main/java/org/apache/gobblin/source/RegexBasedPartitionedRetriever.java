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
package org.apache.gobblin.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;
import org.apache.gobblin.source.extractor.hadoop.HadoopFsHelper;
import org.apache.gobblin.util.PathUtils;


public class RegexBasedPartitionedRetriever implements PartitionAwareFileRetriever {
  private static final Logger LOGGER = LoggerFactory.getLogger(RegexBasedPartitionedRetriever.class);

  private Pattern pattern;
  private HadoopFsHelper helper;
  private Path sourceDir;
  private final String expectedExtension;
  private Duration leadTime;
  private boolean schemaInSourceDir;
  private String schemaFile;

  public RegexBasedPartitionedRetriever(String expectedExtension) {
    this.expectedExtension = expectedExtension;
  }

  @Override
  public void init(SourceState state) {
    String regexPattern = state.getProp(PartitionedFileSourceBase.DATE_PARTITIONED_SOURCE_PARTITION_PATTERN);
    Preconditions.checkNotNull(regexPattern, "Must specify a regex pattern in " +
      PartitionedFileSourceBase.DATE_PARTITIONED_SOURCE_PARTITION_PATTERN
    );

    this.leadTime = PartitionAwareFileRetrieverUtils.getLeadTimeDurationFromConfig(state);
    this.pattern = Pattern.compile(regexPattern);
    this.helper = new HadoopFsHelper(state);
    this.sourceDir = new Path(state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY));
    this.schemaInSourceDir = state.getPropAsBoolean(ConfigurationKeys.SCHEMA_IN_SOURCE_DIR,
        ConfigurationKeys.DEFAULT_SCHEMA_IN_SOURCE_DIR);
    this.schemaFile = this.schemaInSourceDir ? state.getProp(ConfigurationKeys.SCHEMA_FILENAME,
        ConfigurationKeys.DEFAULT_SCHEMA_FILENAME) : "";
  }

  @Override
  public long getWatermarkFromString(String watermark) {
    // Subclasses can override this with DateTimeFormatter and/or specify via config
    return Long.parseLong(watermark);
  }

  protected String extractWatermarkFromDirectory(String directoryName) {
    Matcher matcher = pattern.matcher(directoryName);
    if (!matcher.matches() || matcher.groupCount() < 1)  {
      throw new IllegalArgumentException(directoryName + " does not match regex " + pattern.toString());
    }

    return matcher.group(1);
  }

  @Override
  public long getWatermarkIncrementMs() {
    return 1;
  }

  @Override
  public List<FileInfo> getFilesToProcess(long minWatermark, int maxFilesToReturn)
      throws IOException {
    // This implementation assumes snapshots are always in the root directory and the number of them
    // remains relatively small
    long maxAllowedWatermark = new DateTime().minus(leadTime).getMillis();

    try {
      this.helper.connect();
      FileSystem fs = helper.getFileSystem();
      List<FileInfo> filesToProcess = new ArrayList<>();

      List<FileInfo> outerDirectories = getOuterDirectories(fs, minWatermark, maxAllowedWatermark);
      for (FileInfo outerDirectory: outerDirectories) {
        FileStatus[] files = fs.listStatus(
            new Path(outerDirectory.getFilePath()),
            getFileFilter()
            );
        for (FileStatus file: files) {
          filesToProcess.add(new FileInfo(
              file.getPath().toString(),
              file.getLen(),
              outerDirectory.getWatermarkMsSinceEpoch(),
              outerDirectory.getPartitionName()
          ));
        }

        if (filesToProcess.size() > maxFilesToReturn) {
          break;
        }
      }

      return filesToProcess;
    } catch (FileBasedHelperException e) {
      throw new IOException("Error initializing Hadoop connection", e);
    }
  }

  private List<FileInfo> getOuterDirectories(FileSystem fs, long minWatermark, long maxAllowedWatermark) throws IOException {
    LOGGER.debug("Listing contents of {}", sourceDir);

    FileStatus[] fileStatus = fs.listStatus(sourceDir);
    List<FileInfo> outerDirectories = new ArrayList<>();

    for (FileStatus file: fileStatus) {
      if (!file.isDirectory()) {
        LOGGER.debug("Skipping non-directory {}", file.getPath().toUri());
        continue;
      }

      try {
        long watermark = getWatermarkFromString(
            extractWatermarkFromDirectory(file.getPath().getName())
        );
        if (watermark > minWatermark && watermark < maxAllowedWatermark) {
          LOGGER.info("Processing directory {} with watermark {}",
              file.getPath(),
              watermark);
          outerDirectories.add(new FileInfo(
              file.getPath().toString(),
              0,
              watermark, PathUtils.relativizePath(file.getPath(), sourceDir).toString()
          ));
        } else {
          LOGGER.info("Ignoring directory {} - watermark {} is not between minWatermark {} and (now-leadTime) {}",
              file.getPath(), watermark, minWatermark, maxAllowedWatermark);
        }
      } catch (IllegalArgumentException e) {
        LOGGER.info("Directory {} ({}) does not match pattern {}; skipping", file.getPath().getName(),
            file.getPath(),
            this.pattern.toString());
      }
    }

    Collections.sort(outerDirectories);
    return outerDirectories;
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
        return path.getName().endsWith(extension) &&
            !(schemaInSourceDir && path.getName().equals(schemaFile)) ;
      }
    };
  }
}
