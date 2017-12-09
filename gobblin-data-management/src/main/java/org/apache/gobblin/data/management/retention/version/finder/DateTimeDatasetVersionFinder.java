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

package org.apache.gobblin.data.management.retention.version.finder;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;

import org.apache.gobblin.data.management.retention.version.DatasetVersion;
import org.apache.gobblin.data.management.retention.version.TimestampedDatasetVersion;


/**
 * @deprecated
 * See javadoc for {@link org.apache.gobblin.data.management.version.finder.DateTimeDatasetVersionFinder}.
 */
@Deprecated
public class DateTimeDatasetVersionFinder extends DatasetVersionFinder<TimestampedDatasetVersion> {

  private final org.apache.gobblin.data.management.version.finder.DateTimeDatasetVersionFinder realVersionFinder;

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

  public DateTimeDatasetVersionFinder(FileSystem fs, Properties props) {
    super(fs, convertDeprecatedProperties(props));
    this.realVersionFinder = new org.apache.gobblin.data.management.version.finder.DateTimeDatasetVersionFinder(fs, convertDeprecatedProperties(props));
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }

  @Override
  public Path globVersionPattern() {
    return this.realVersionFinder.globVersionPattern();
  }

  @Override
  public TimestampedDatasetVersion getDatasetVersion(Path pathRelativeToDatasetRoot, FileStatus versionFileStatus) {
    org.apache.gobblin.data.management.version.TimestampedDatasetVersion timestampedDatasetVersion =
        this.realVersionFinder.getDatasetVersion(pathRelativeToDatasetRoot, versionFileStatus);
    if (timestampedDatasetVersion != null) {
      return new TimestampedDatasetVersion(timestampedDatasetVersion);
    }
    return null;
  }

  // This Method will never be called. It exists because the deprecated super class org.apache.gobblin.data.management.retention.version.finder.DatasetVersionFinder
  // requires it. getDatasetVersion(Path pathRelativeToDatasetRoot, FileStatus versionFileStatus) will be called instead
  @Override
  public TimestampedDatasetVersion getDatasetVersion(Path pathRelativeToDatasetRoot, Path fullPath) {
    throw new UnsupportedOperationException(
        "This method should not be called. getDatasetVersion(Path pathRelativeToDatasetRoot, FileStatus versionFileStatus) "
        + "should have been called instead");
  }

  /**
   * This conversion is required because the deprecated keys {@value #RETENTION_DATE_TIME_PATTERN_KEY} and
   * {@value #RETENTION_DATE_TIME_PATTERN_TIMEZONE_KEY} are not TypeSafe compatible.
   * The key {@value #RETENTION_DATE_TIME_PATTERN_TIMEZONE_KEY} overwrites {@value #RETENTION_DATE_TIME_PATTERN_KEY}
   * when converted from props to {@link Config}
   */
  private static Properties convertDeprecatedProperties(Properties props) {
    if (props.containsKey(RETENTION_DATE_TIME_PATTERN_KEY)) {
      props.setProperty(org.apache.gobblin.data.management.version.finder.DateTimeDatasetVersionFinder.DATE_TIME_PATTERN_KEY, props.getProperty(RETENTION_DATE_TIME_PATTERN_KEY));
      props.remove(RETENTION_DATE_TIME_PATTERN_KEY);
    }
    if (props.containsKey(RETENTION_DATE_TIME_PATTERN_TIMEZONE_KEY)) {
      props.setProperty(org.apache.gobblin.data.management.version.finder.DateTimeDatasetVersionFinder.DATE_TIME_PATTERN_TIMEZONE_KEY, props.getProperty(RETENTION_DATE_TIME_PATTERN_TIMEZONE_KEY));
      props.remove(RETENTION_DATE_TIME_PATTERN_TIMEZONE_KEY);
    }
    return props;
  }


}
