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

package gobblin.util.recordcount;

import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import gobblin.util.RecordCountProvider;


/**
 * Implementation of {@link RecordCountProvider}, which provides record count from file path.
 * The file path should follow the pattern: {Filename}.{RecordCount}.{Extension}.
 * For example, given a file path: "/a/b/c/file.123.avro", the record count will be 123.
 */
public class IngestionRecordCountProvider extends RecordCountProvider {

  private static final String SEPARATOR = ".";

  /**
   * Construct a new file path by appending record count to the filename of the given file path, separated by SEPARATOR.
   * For example, given path: "/a/b/c/file.avro" and record count: 123,
   * the new path returned will be: "/a/b/c/file.123.avro"
   */
  public static String constructFilePath(String oldFilePath, long recordCounts) {
    return new Path(new Path(oldFilePath).getParent(), Files.getNameWithoutExtension(oldFilePath).toString() + SEPARATOR
        + recordCounts + SEPARATOR + Files.getFileExtension(oldFilePath)).toString();
  }

  /**
   * The record count should be the last component before the filename extension.
   */
  @Override
  public long getRecordCount(Path filepath) {
    String[] components = filepath.getName().split(Pattern.quote(SEPARATOR));
    Preconditions.checkArgument(components.length >= 2 && StringUtils.isNumeric(components[components.length - 2]),
        String.format("Filename %s does not follow the pattern: FILENAME.RECORDCOUNT.EXTENSION", filepath));
    return Long.parseLong(components[components.length - 2]);
  }
}
