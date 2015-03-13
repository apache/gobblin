/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;


/**
 * A utility class for generating script to move the heap dump .prof files to HDFS for hadoop tasks, when Java heap out of memory error is thrown.
 *
 */
public class HeapDumpForTaskUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HeapDumpForTaskUtils.class);
  private static final String DUMP_FOLDER = "/dumps/";

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
   * @throws IOException
   */
  public static void generateDumpScript(Path dumpScript, FileSystem fs, String heapFileName) throws IOException {
    BufferedWriter scriptWriter = null;
    Closer closer = Closer.create();
    try {
      if (fs.exists(dumpScript)) {
        LOG.info("Heap dump script already exists: " + dumpScript);
      } else {
        Path dumpDir = new Path(dumpScript.getParent() + DUMP_FOLDER);
        if (fs.exists(new Path(dumpScript.getParent() + DUMP_FOLDER))) {
          fs.create(dumpDir);
        }
        scriptWriter = closer.register(new BufferedWriter(new OutputStreamWriter(fs.create(dumpScript))));
        scriptWriter.write("#!/bin/sh\n" + "hadoop dfs -put " + heapFileName + " " + dumpDir + "${PWD//\\//_}.hprof");
        scriptWriter.flush();
        Runtime.getRuntime().exec("hadoop fs -chmod 755 " + dumpScript);
      }
    } catch (IOException e) {
      LOG.error("Heap dump script is not generated successfully.");
      if (fs.exists(dumpScript)) {
        fs.delete(dumpScript, true);
      }
    } finally {
      closer.close();
    }
  }
}
