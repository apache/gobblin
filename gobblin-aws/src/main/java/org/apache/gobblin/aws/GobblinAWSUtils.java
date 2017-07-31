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

package org.apache.gobblin.aws;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;

import java.util.concurrent.TimeUnit;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.lang.StringUtils;
import org.quartz.utils.FindbugsSuppressWarnings;
import org.slf4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import org.apache.gobblin.annotation.Alpha;


/**
 * An util class for Gobblin on AWS misc functionality.
 *
 * @author Abhishek Tiwari
 */
@Alpha
public class GobblinAWSUtils {
  private static final long DEFAULT_EXECUTOR_SERVICE_SHUTDOWN_TIME_IN_MINUTES = 2;

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

  /***
   * Initiates an orderly shutdown in which previously submitted
   * tasks are executed, but no new tasks are accepted.
   * Invocation has no additional effect if already shut down.
   *
   * This also blocks until all tasks have completed execution
   * request, or the timeout occurs, or the current thread is
   * interrupted, whichever happens first.
   * @param clazz {@link Class} that invokes shutdown on the {@link ExecutorService}.
   * @param executorService {@link ExecutorService} to shutdown.
   * @param logger {@link Logger} to log shutdown for invoking class.
   * @throws InterruptedException if shutdown is interrupted.
   */
  public static void shutdownExecutorService(Class clazz,
      ExecutorService executorService, Logger logger) throws InterruptedException{
    executorService.shutdown();
    if (!executorService.awaitTermination(DEFAULT_EXECUTOR_SERVICE_SHUTDOWN_TIME_IN_MINUTES, TimeUnit.MINUTES)) {
      logger.warn("Executor service shutdown timed out.");
      List<Runnable> pendingTasks = executorService.shutdownNow();
      logger.warn(String
          .format("%s was shutdown instantly. %s tasks were not executed: %s", clazz.getName(), pendingTasks.size(),
              StringUtils.join(pendingTasks, ",")));
    }
  }
}
