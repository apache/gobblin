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

package gobblin.aws;

import java.io.File;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.lang.StringUtils;
import org.quartz.utils.FindbugsSuppressWarnings;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import gobblin.annotation.Alpha;


/**
 * An util class for Gobblin on AWS misc functionality.
 *
 * @author Abhishek Tiwari
 */
@Alpha
public class GobblinAWSUtils {
  /***
   * Append a slash ie / at the end of input string.
   *
   * @param inputString Input string to append a slash to
   * @return String with slash appened
   */
  public static String appendSlash(String inputString) {
    Preconditions.checkNotNull(inputString);

    if (inputString.endsWith("/")) {
      return inputString;
    }

    return inputString + "/";
  }

  /***
   * List and generate classpath string from paths.
   *
   * Note: This is currently unused, and will be brought in to use with custom Gobblin AMI
   *
   * @param paths Paths to list
   * @return Classpath string
   */
  public static String getClasspathFromPaths(File... paths) {
    Preconditions.checkNotNull(paths);

    final StringBuilder classpath = new StringBuilder();
    boolean isFirst = true;
    for (File path : paths) {
      if (!isFirst) {
        classpath.append(":");
      }
      final String subClasspath = getClasspathFromPath(path);
      if (subClasspath.length() > 0) {
        classpath.append(subClasspath);
        isFirst = false;
      }
    }

    return classpath.toString();
  }

  private static String getClasspathFromPath(File path) {
    if (null == path) {
      return StringUtils.EMPTY;
    }
    if (!path.isDirectory()) {
      return path.getAbsolutePath();
    }

    return Joiner.on(":").skipNulls().join(path.list(FileFileFilter.FILE));
  }

  /***
   * Encodes String data using the base64 algorithm and does not chunk the output.
   *
   * @param data String to be encoded.
   * @return Encoded String.
   */
  @FindbugsSuppressWarnings("DM_DEFAULT_ENCODING")
  public static String encodeBase64(String data) {
    final byte[] encodedBytes = Base64.encodeBase64(data.getBytes());

    return new String(encodedBytes);
  }
}
