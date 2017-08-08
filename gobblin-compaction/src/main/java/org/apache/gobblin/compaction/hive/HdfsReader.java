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

package org.apache.gobblin.compaction.hive;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class for read operations on HDFS.
 */
public class HdfsReader extends HdfsIO {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsReader.class);

  public HdfsReader(String filePathInHdfs) throws IOException {
    super(filePathInHdfs);
  }

  public InputStream getInputStream() throws IOException {
    return this.fileSystem.open(new Path(this.filePathInHdfs));
  }

  public FsInput getFsInput() throws IOException {
    Path path = new Path(this.filePathInHdfs);
    Configuration conf = getConfiguration();
    return new FsInput(path, conf);
  }

  public static String getFirstDataFilePathInDir(String dirInHdfs) throws IOException {
    FileStatus[] fileStatuses = getFileSystem().listStatus(new Path(dirInHdfs));
    for (FileStatus fileStatus : fileStatuses) {
      Path dataFilePath = fileStatus.getPath();
      if (!fileStatus.isDirectory() && !dataFilePath.getName().startsWith("_")) {
        return dataFilePath.toString();
      }
    }
    String message = dirInHdfs + " does not contain a valid data file.";
    LOG.error(message);
    throw new RuntimeException(message);
  }
}
