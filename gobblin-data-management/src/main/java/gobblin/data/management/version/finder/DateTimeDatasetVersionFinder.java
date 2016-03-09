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

package gobblin.data.management.version.finder;

import gobblin.configuration.ConfigurationKeys;
import gobblin.data.management.version.FileSystemDatasetVersion;
import gobblin.data.management.version.TimestampedDatasetVersion;

import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


/**
 * {@link gobblin.data.management.version.finder.DatasetVersionFinder} for datasets based on path timestamps.
 * Uses a datetime pattern to find dataset versions from the dataset path
 * and parse the {@link org.joda.time.DateTime} representing the version.
 */
public class DateTimeDatasetVersionFinder extends DatasetVersionFinder<TimestampedDatasetVersion> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DateTimeDatasetVersionFinder.class);

  public static final String DATE_TIME_PATTERN_KEY = "gobblin.dataset.version.datetime.pattern";
  public static final String DATE_TIME_PATTERN_TIMEZONE_KEY = "gobblin.dataset.version.datetime.pattern.timezone";
  public static final String DEFAULT_DATE_TIME_PATTERN_TIMEZONE = ConfigurationKeys.PST_TIMEZONE_NAME;

  /**
   * @deprecated use {@link #DATE_TIME_PATTERN_KEY} instead.
   */
  @Deprecated
  public static final String RETENTION_DATE_TIME_PATTERN_KEY = "gobblin.retention.datetime.pattern";
  /**
   * @deprecated use {@link #DATE_TIME_PATTERN_TIMEZONE_KEY} instead.
   */
  @Deprecated
  public static final String RETENTION_DATE_TIME_PATTERN_TIMEZONE_KEY = "gobblin.retention.datetime.pattern.timezone";

  private final Path globPattern;
  private final DateTimeFormatter formatter;

  public DateTimeDatasetVersionFinder(FileSystem fs, Properties props) {
    super(fs, props);
    Preconditions.checkArgument(props.containsKey(DATE_TIME_PATTERN_KEY)
        || props.containsKey(RETENTION_DATE_TIME_PATTERN_KEY));
    String pattern;
    if (props.containsKey(DATE_TIME_PATTERN_KEY)) {
      pattern = props.getProperty(DATE_TIME_PATTERN_KEY);
    } else {
      pattern = props.getProperty(RETENTION_DATE_TIME_PATTERN_KEY);
    }
    this.globPattern = new Path(pattern.replaceAll("[^/]+", "*"));
    LOGGER.debug(String.format("Setting timezone for patthern: %s. By default it is %s", pattern,
        DEFAULT_DATE_TIME_PATTERN_TIMEZONE));

    if (props.containsKey(DATE_TIME_PATTERN_TIMEZONE_KEY)) {
      this.formatter =
          DateTimeFormat.forPattern(pattern).withZone(
              DateTimeZone.forID(props.getProperty(DATE_TIME_PATTERN_TIMEZONE_KEY)));
    } else if (props.containsKey(RETENTION_DATE_TIME_PATTERN_TIMEZONE_KEY)) {
      this.formatter =
          DateTimeFormat.forPattern(pattern).withZone(
              DateTimeZone.forID(props.getProperty(RETENTION_DATE_TIME_PATTERN_TIMEZONE_KEY)));
    } else {
      this.formatter =
          DateTimeFormat.forPattern(pattern).withZone(DateTimeZone.forID(DEFAULT_DATE_TIME_PATTERN_TIMEZONE));
    }
  }

  @Override
  public Class<? extends FileSystemDatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }

  /**
   * Obtained by replacing all non-slash characters in datetime pattern by *.
   * E.g. yyyy/MM/dd/hh/mm -> *\/*\/*\/*\/*
   */
  @Override
  public Path globVersionPattern() {
    return this.globPattern;
  }

  /**
   * Parse {@link org.joda.time.DateTime} from {@link org.apache.hadoop.fs.Path} using datetime pattern.
   */
  @Override
  public TimestampedDatasetVersion getDatasetVersion(Path pathRelativeToDatasetRoot, Path fullPath) {
    try {
      return new TimestampedDatasetVersion(this.formatter.parseDateTime(pathRelativeToDatasetRoot.toString()), fullPath);
    } catch (IllegalArgumentException exception) {
      LOGGER.warn("Candidate dataset version at " + pathRelativeToDatasetRoot
          + " does not match expected pattern. Ignoring.");
      return null;
    }
  }

}
