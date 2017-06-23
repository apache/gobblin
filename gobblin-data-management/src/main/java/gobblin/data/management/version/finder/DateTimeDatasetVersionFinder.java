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

package gobblin.data.management.version.finder;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.data.management.version.FileStatusTimestampedDatasetVersion;
import gobblin.data.management.version.FileSystemDatasetVersion;
import gobblin.data.management.version.TimestampedDatasetVersion;


/**
 * {@link gobblin.data.management.version.finder.DatasetVersionFinder} for datasets based on path timestamps.
 * Uses a datetime pattern to find dataset versions from the dataset path
 * and parse the {@link org.joda.time.DateTime} representing the version.
 */
public class DateTimeDatasetVersionFinder extends AbstractDatasetVersionFinder<TimestampedDatasetVersion> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DateTimeDatasetVersionFinder.class);

  /**
   * Date pattern of the partition. E.g. yyyy/MM/dd/hh/mm or yyyy/MM/dd
   */
  public static final String DATE_TIME_PATTERN_KEY = "version.datetime.pattern";
  /**
   * Time zone to be used E.g. UTC
   */
  public static final String DATE_TIME_PATTERN_TIMEZONE_KEY = "version.datetime.timezone";
  /**
   * By default the globPattern is obtained by replacing all non-slash characters in datetime pattern by *.
   * E.g. yyyy/MM/dd/hh/mm -> *\/*\/*\/*\/*.
   * If this key is set, we use this globPattern to search for version
   */
  public static final String OPTIONAL_GLOB_PATTERN_TIMEZONE_KEY = "version.globPattern";

  public static final String DEFAULT_DATE_TIME_PATTERN_TIMEZONE = ConfigurationKeys.PST_TIMEZONE_NAME;

  private final Path globPattern;
  protected final DateTimeFormatter formatter;
  private final String datePartitionPattern;

  public DateTimeDatasetVersionFinder(FileSystem fs, Config config) {
    super(fs);
    Preconditions.checkArgument(config.hasPath(DATE_TIME_PATTERN_KEY) , "Missing required property " + DATE_TIME_PATTERN_KEY);
    String pattern = config.getString(DATE_TIME_PATTERN_KEY);

    if (config.hasPath(OPTIONAL_GLOB_PATTERN_TIMEZONE_KEY)) {
      this.globPattern = new Path(config.getString(OPTIONAL_GLOB_PATTERN_TIMEZONE_KEY));
    } else {
      this.globPattern = new Path(pattern.replaceAll("[^/]+", "*"));
    }

    LOGGER.debug(String.format("Setting timezone for patthern: %s. By default it is %s", pattern,
        DEFAULT_DATE_TIME_PATTERN_TIMEZONE));

    if (config.hasPath(DATE_TIME_PATTERN_TIMEZONE_KEY)) {
      this.formatter =
          DateTimeFormat.forPattern(pattern).withZone(
              DateTimeZone.forID(config.getString(DATE_TIME_PATTERN_TIMEZONE_KEY)));
    } else {
      this.formatter =
          DateTimeFormat.forPattern(pattern).withZone(DateTimeZone.forID(DEFAULT_DATE_TIME_PATTERN_TIMEZONE));
    }

    this.datePartitionPattern = pattern;
  }

  public DateTimeDatasetVersionFinder(FileSystem fs, Properties props) {
    this(fs, ConfigFactory.parseProperties(props));
  }

  @Override
  public Class<? extends FileSystemDatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }

  /**
   * Obtained by replacing all non-slash characters in datetime pattern by *.
   * E.g. yyyy/MM/dd/hh/mm -> *\/*\/*\/*\/*
   * Or glob pattern at {@value #OPTIONAL_GLOB_PATTERN_TIMEZONE_KEY} if set.
   */
  @Override
  public Path globVersionPattern() {
    return this.globPattern;
  }

  /**
   * Parse {@link org.joda.time.DateTime} from {@link org.apache.hadoop.fs.Path} using datetime pattern.
   */
  @Override
  public TimestampedDatasetVersion getDatasetVersion(Path pathRelativeToDatasetRoot, FileStatus versionFileStatus) {

    String dateTimeString = null;
    try {
      // pathRelativeToDatasetRoot can be daily/2016/03/02 or 2016/03/02. In either case we need to pick 2016/03/02 as version
      dateTimeString =
          StringUtils.substring(pathRelativeToDatasetRoot.toString(), pathRelativeToDatasetRoot.toString().length()
              - this.datePartitionPattern.length());

      return new FileStatusTimestampedDatasetVersion(this.formatter.parseDateTime(dateTimeString), versionFileStatus);

    } catch (IllegalArgumentException exception) {
      LOGGER.warn(String.format(
          "Candidate dataset version with pathRelativeToDatasetRoot: %s has inferred dataTimeString:%s. "
              + "It does not match expected datetime pattern %s. Ignoring.", pathRelativeToDatasetRoot, dateTimeString,
          this.datePartitionPattern));
      return null;
    }
  }
}
