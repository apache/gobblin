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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.util.HadoopUtils;


/**
 * A class for write operations on HDFS.
 */
public class HdfsWriter extends HdfsIO {

  public HdfsWriter(String filePathInHdfs) throws IOException {
    super(filePathInHdfs);
  }

  public void write(String text) throws IOException {
    String dirInHdfs = getDirInHdfs();
    this.fileSystem.mkdirs(new Path(dirInHdfs));

    try (FSDataOutputStream fout = this.fileSystem.create(new Path(this.filePathInHdfs))) {
      fout.writeChars(text);
    }
  }

  private String getDirInHdfs() {
    return new Path(this.filePathInHdfs).getParent().toString();
  }

  public boolean delete() throws IllegalArgumentException, IOException {
    return this.fileSystem.delete(new Path(this.filePathInHdfs), true);
  }

  public static void moveSelectFiles(String extension, String source, String destination) throws IOException {
    FileSystem fs = getFileSystem();
    fs.mkdirs(new Path(destination));
    FileStatus[] fileStatuses = fs.listStatus(new Path(source));
    for (FileStatus fileStatus : fileStatuses) {
      Path path = fileStatus.getPath();
      if (!fileStatus.isDirectory() && path.toString().toLowerCase().endsWith(extension.toLowerCase())) {
        HadoopUtils.deleteIfExists(fs, new Path(destination), true);
        HadoopUtils.copyPath(fs, path, fs, new Path(destination), getConfiguration());
      }
    }
  }
}
