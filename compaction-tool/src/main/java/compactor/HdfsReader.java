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

package compactor;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class for read operations on HDFS.
 *
 * @author ziliu
 */
public class HdfsReader extends HdfsIO {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsReader.class);

  public HdfsReader(String filePathInHdfs) throws IOException {
    super(filePathInHdfs);
  }

  public InputStream getInputStream() throws IOException {
    return this.fileSystem.open(new Path(filePathInHdfs));
  }

  public FsInput getFsInput() throws IOException {
    Path path = new Path(filePathInHdfs);
    Configuration conf = getConfiguration();
    return new FsInput(path, conf);
  }

  public static String getFirstDataFilePathInDir(String dirInHdfs) throws IOException {
    RemoteIterator<LocatedFileStatus> iterator = getFileSystem().listFiles(new Path(dirInHdfs), false);
    PathFilter pathFilter = new GlobFilter("_*");
    while (iterator.hasNext()) {
      Path dataFilePath = iterator.next().getPath();
      if (!pathFilter.accept(dataFilePath)) {
        return dataFilePath.toString();
      }
    }
    String message = dirInHdfs + " does not contain a valid data file.";
    LOG.error(message);
    throw new RuntimeException(message);
  }
}
