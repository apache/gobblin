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

package gobblin.data.management.retention.version.finder;

import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.data.management.retention.DatasetCleaner;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.TimestampedDatasetVersion;


/**
 * {@link gobblin.data.management.retention.version.finder.DatasetVersionFinder} for datasets based on path timestamps.
 * Uses a datetime pattern to find dataset versions from the dataset path
 * and parse the {@link org.joda.time.DateTime} representing the version.
 */
public class DateTimeDatasetVersionFinder extends DatasetVersionFinder<TimestampedDatasetVersion> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DateTimeDatasetVersionFinder.class);

  public static final String DATE_TIME_PATTERN_KEY = DatasetCleaner.CONFIGURATION_KEY_PREFIX + "datetime.pattern";
  public static final String DATE_TIME_PATTERN_TIMEZONE_KEY =
      DatasetCleaner.CONFIGURATION_KEY_PREFIX + "datetime.pattern.timezone";
  public static final String DEFAULT_DATE_TIME_PATTERN_TIMEZONE = ConfigurationKeys.PST_TIMEZONE_NAME;

  private final Path globPattern;
  private final DateTimeFormatter formatter;

  public DateTimeDatasetVersionFinder(FileSystem fs, Properties props) {
    super(fs, props);
    String pattern = props.getProperty(DATE_TIME_PATTERN_KEY);
    this.globPattern = new Path(pattern.replaceAll("[^/]+", "*"));
    LOGGER.debug(String.format("Setting timezone for patthern: %s. By default it is %s", pattern,
        DEFAULT_DATE_TIME_PATTERN_TIMEZONE));
    this.formatter = DateTimeFormat.forPattern(pattern).withZone(
        DateTimeZone.forID(props.getProperty(DATE_TIME_PATTERN_TIMEZONE_KEY, DEFAULT_DATE_TIME_PATTERN_TIMEZONE)));

  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
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
      return new TimestampedDatasetVersion(this.formatter.parseDateTime(pathRelativeToDatasetRoot.toString()),
          fullPath);
    } catch (IllegalArgumentException exception) {
      LOGGER.warn(
          "Candidate dataset version at " + pathRelativeToDatasetRoot + " does not match expected pattern. Ignoring.");
      return null;
    }
  }

}
