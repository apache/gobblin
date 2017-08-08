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

package org.apache.gobblin.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;


/**
 * A utility class for generating script to move the heap dump .prof files to HDFS for hadoop tasks, when Java heap out of memory error is thrown.
 */
public class HeapDumpForTaskUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HeapDumpForTaskUtils.class);
  private static final String DUMP_FOLDER = "dumps";

  /**
   * Generate the dumpScript, which is used when OOM error is thrown during task execution.
   * The current content dumpScript puts the .prof files to the DUMP_FOLDER within the same directory of the dumpScript.
   *
   * User needs to add the following options to the task java.opts:
   *
   * -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./heapFileName.hprof -XX:OnOutOfMemoryError=./dumpScriptFileName
   *
   * @param dumpScript The path to the dumpScript, which needs to be added to the Distributed cache.
   * To use it, simply put the path of dumpScript to the gobblin config: job.hdfs.files.
   * @param fs File system
   * @param heapFileName the name of the .prof file.
   * @param chmod chmod for the dump script. For hdfs file, e.g, "hadoop fs -chmod 755"
   * @throws IOException
   */
  public static void generateDumpScript(Path dumpScript, FileSystem fs, String heapFileName, String chmod)
      throws IOException {
    if (fs.exists(dumpScript)) {
      LOG.info("Heap dump script already exists: " + dumpScript);
      return;
    }

    try (BufferedWriter scriptWriter =
        new BufferedWriter(new OutputStreamWriter(fs.create(dumpScript), ConfigurationKeys.DEFAULT_CHARSET_ENCODING))) {
      Path dumpDir = new Path(dumpScript.getParent(), DUMP_FOLDER);
      if (!fs.exists(dumpDir)) {
        fs.mkdirs(dumpDir);
      }

      scriptWriter.write("#!/bin/sh\n");
      scriptWriter.write("if [ -n \"$HADOOP_PREFIX\" ]; then\n");
      scriptWriter
          .write("  ${HADOOP_PREFIX}/bin/hadoop dfs -put " + heapFileName + " " + dumpDir + "/${PWD//\\//_}.hprof\n");
      scriptWriter.write("else\n");
      scriptWriter
          .write("  ${HADOOP_HOME}/bin/hadoop dfs -put " + heapFileName + " " + dumpDir + "/${PWD//\\//_}.hprof\n");
      scriptWriter.write("fi\n");

    } catch (IOException ioe) {
      LOG.error("Heap dump script is not generated successfully.");
      if (fs.exists(dumpScript)) {
        fs.delete(dumpScript, true);
      }
      throw ioe;
    }
    Runtime.getRuntime().exec(chmod + " " + dumpScript);
  }
}
