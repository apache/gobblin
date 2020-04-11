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

package org.apache.gobblin.compaction.mapreduce.orc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.gobblin.compaction.mapreduce.avro.MRCompactorAvroKeyDedupJobRunner;
import org.apache.gobblin.util.FileListUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;


public class OrcUtils {
  // For Util class to prevent initialization
  private OrcUtils() {

  }

  public static TypeDescription getTypeDescriptionFromFile(Configuration conf, Path orcFilePath) throws IOException {
    return getRecordReaderFromFile(conf, orcFilePath).getSchema();
  }

  public static Reader getRecordReaderFromFile(Configuration conf, Path orcFilePath) throws IOException {
    return OrcFile.createReader(orcFilePath, new OrcFile.ReaderOptions(conf));
  }

  public static TypeDescription getNewestSchemaFromSource(Job job, FileSystem fs) throws IOException {
    Path[] sourceDirs = FileInputFormat.getInputPaths(job);
    if (sourceDirs.length == 0) {
      throw new IllegalStateException("There should be at least one directory specified for the MR job");
    }

    List<FileStatus> files = new ArrayList<FileStatus>();

    for (Path sourceDir : sourceDirs) {
      files.addAll(FileListUtils.listFilesRecursively(fs, sourceDir));
    }
    Collections.sort(files, new MRCompactorAvroKeyDedupJobRunner.LastModifiedDescComparator());

    TypeDescription resultSchema;
    for (FileStatus status : files) {
      resultSchema = getTypeDescriptionFromFile(job.getConfiguration(), status.getPath());
      if (resultSchema != null) {
        return resultSchema;
      }
    }

    throw new IllegalStateException(
        String.format("There's no file carrying orc file schema in the list of directories: %s", Arrays.asList(sourceDirs)));
  }
}
