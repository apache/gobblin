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

import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.data.management.retention.version.DatasetVersion;
import org.apache.gobblin.data.management.retention.version.StringDatasetVersion;


/**
 * @deprecated
 * See javadoc for {@link org.apache.gobblin.data.management.version.finder.WatermarkDatasetVersionFinder}.
 */
@Slf4j
@Deprecated
public class WatermarkDatasetVersionFinder extends DatasetVersionFinder<StringDatasetVersion> {

  private final org.apache.gobblin.data.management.version.finder.WatermarkDatasetVersionFinder realVersionFinder;

  public static final String DEPRECATED_WATERMARK_REGEX_KEY = "gobblin.retention.watermark.regex";

  public WatermarkDatasetVersionFinder(FileSystem fs, Properties props) {
    super(fs, props);
    this.realVersionFinder =
        new org.apache.gobblin.data.management.version.finder.WatermarkDatasetVersionFinder(fs, convertDeprecatedProperties(props));
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return StringDatasetVersion.class;
  }

  @Override
  public Path globVersionPattern() {
    return this.realVersionFinder.globVersionPattern();
  }

  @Override
  public StringDatasetVersion getDatasetVersion(Path pathRelativeToDatasetRoot, Path fullPath) {
    org.apache.gobblin.data.management.version.StringDatasetVersion stringDatasetVersion =
        this.realVersionFinder.getDatasetVersion(pathRelativeToDatasetRoot, fullPath);
    if (stringDatasetVersion != null) {
      return new StringDatasetVersion(stringDatasetVersion);
    }
    return null;
  }

  public static Properties convertDeprecatedProperties(Properties props) {
    if (props.containsKey(DEPRECATED_WATERMARK_REGEX_KEY)) {
      log.info(String.format("Found deprecated key %s. Replacing it with %s", DEPRECATED_WATERMARK_REGEX_KEY,
          org.apache.gobblin.data.management.version.finder.WatermarkDatasetVersionFinder.WATERMARK_REGEX_KEY));

      props.setProperty(org.apache.gobblin.data.management.version.finder.WatermarkDatasetVersionFinder.WATERMARK_REGEX_KEY,
          props.getProperty(DEPRECATED_WATERMARK_REGEX_KEY));
      props.remove(DEPRECATED_WATERMARK_REGEX_KEY);
    }

    return props;
  }
}
