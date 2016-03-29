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

package gobblin.hadoop.token;

import java.io.File;
import java.io.IOException;

import gobblin.configuration.State;


/**
 * A utility class for obtain Hadoop tokens and Hive metastore tokens for Azkaban jobs.
 *
 * <p>
 *   This class is compatible with Hadoop 1.
 * </p>
 *
 * @author ibuenros
 */
public class TokenUtils {

  /**
   * Get Hadoop tokens (tokens for job history server, job tracker and HDFS).
   *
   * @param state A {@link State} object that should contain property {@link #USER_TO_PROXY},
   * {@link #KEYTAB_USER} and {@link #KEYTAB_LOCATION}. To obtain tokens for
   * other namenodes, use property {@link #OTHER_NAMENODES} with comma separated HDFS URIs.
   */
  public static File getHadoopTokens(final State state) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }
}
