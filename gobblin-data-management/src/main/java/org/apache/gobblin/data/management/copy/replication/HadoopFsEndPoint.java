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

package gobblin.data.management.copy.replication;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;

import gobblin.util.HadoopUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class HadoopFsEndPoint implements EndPoint{

  /**
   *
   * @return the hadoop cluster name for {@link EndPoint}s on Hadoop File System
   */
  public abstract String getClusterName();

  /**
   * @return the hadoop cluster FileSystem URI
   */
  public abstract URI getFsURI();

  /**
   *
   * @return Deepest {@link org.apache.hadoop.fs.Path} that contains all files in the dataset.
   */
  public abstract Path getDatasetPath();

  public abstract Config getSelectionConfig();

  /**
   * A helper utility for data/filesystem availability checking
   * @param path The path to be checked. For fs availability checking, just use "/"
   * @return If the filesystem/path exists or not.
   */
  public boolean isPathAvailable(Path path){
    try {
      Configuration conf = HadoopUtils.newConfiguration();
      FileSystem fs = FileSystem.get(this.getFsURI(), conf);
      if (fs.exists(path)) {
        return true;
      } else {
        log.warn("Skipped the problematic FileSystem " + this.getFsURI());
        return false;
      }
    } catch (IOException ioe) {
      log.warn("Skipped the problematic FileSystem " + this.getFsURI());
      return false;
    }
  }

  @Override
  public boolean isFileSystemAvailable() {
    return isPathAvailable(new Path("/"));
  }

  public boolean isDatasetAvailable(Path datasetPath) {
    return isPathAvailable(datasetPath);
  }
}
