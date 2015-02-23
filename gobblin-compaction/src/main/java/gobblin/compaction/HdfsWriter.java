/* (c) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import com.google.common.io.Closer;


/**
 * A class for write operations on HDFS.
 *
 * @author ziliu
 */
public class HdfsWriter extends HdfsIO {

  public HdfsWriter(String filePathInHdfs) throws IOException {
    super(filePathInHdfs);
  }

  public void write(String text) throws IOException {
    String dirInHdfs = getDirInHdfs();
    this.fileSystem.mkdirs(new Path(dirInHdfs));

    Closer closer = Closer.create();
    try {
      FSDataOutputStream fout = closer.register(this.fileSystem.create(new Path(filePathInHdfs)));
      fout.writeChars(text);
    } finally {
      closer.close();
    }
  }

  private String getDirInHdfs() {
    return new Path(this.filePathInHdfs).getParent().toString();
  }

  public boolean delete() throws IllegalArgumentException, IOException {
    return this.fileSystem.delete(new Path(filePathInHdfs), true);
  }

  public static void moveSelectFiles(String extension, String source, String destination) throws IOException {
    FileSystem fs = getFileSystem();
    fs.mkdirs(new Path(destination));
    //RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(source), false);
    FileStatus[] fileStatuses = fs.listStatus(new Path(source));
    for (FileStatus fileStatus : fileStatuses) {
      Path path = fileStatus.getPath();
      if (!fileStatus.isDir() && path.toString().toLowerCase().endsWith(extension.toLowerCase())) {
        FileUtil.copy(fs, path, fs, new Path(destination), false, true, getConfiguration());
      }
    }
  }
}
