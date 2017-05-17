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
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.source.extractor.ComparableWatermark;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.util.FileListUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SourceHadoopFsEndPoint extends HadoopFsEndPoint{

  @Getter
  private final HadoopFsReplicaConfig rc;

  @Getter
  private final Config selectionConfig;

  private boolean initialized = false;
  private Optional<ComparableWatermark> cachedWatermark = Optional.absent();
  private Collection<FileStatus> allFileStatus = new ArrayList<>();

  public SourceHadoopFsEndPoint(HadoopFsReplicaConfig rc, Config selectionConfig) {
    this.rc = rc;
    this.selectionConfig = selectionConfig;
  }

  @Override
  public synchronized Collection<FileStatus> getFiles() throws IOException{
    if(!this.initialized){
      this.getWatermark();
    }
    return this.allFileStatus;
  }

  @Override
  public synchronized Optional<ComparableWatermark> getWatermark() {
    if(this.initialized) {
      return this.cachedWatermark;
    }
    try {
      long curTs = -1;
      FileSystem fs = FileSystem.get(rc.getFsURI(), new Configuration());

      Collection<Path> validPaths = ReplicationDataValidPathPicker.getValidPaths(this);
      for(Path p: validPaths){
        this.allFileStatus.addAll(FileListUtils.listFilesRecursively(fs, p));
      }

      for (FileStatus f : this.allFileStatus) {
        if (f.getModificationTime() > curTs) {
          curTs = f.getModificationTime();
        }
      }

      ComparableWatermark result = new LongWatermark(curTs);
      this.cachedWatermark = Optional.of(result);

      if (this.cachedWatermark.isPresent()) {
        this.initialized = true;
      }

      return this.cachedWatermark;
    } catch (IOException e) {
      log.error("Error while retrieve the watermark for " + this);
      return this.cachedWatermark;
    }
  }

  @Override
  public boolean isSource() {
    return true;
  }

  @Override
  public String getEndPointName() {
    return ReplicationConfiguration.REPLICATION_SOURCE;
  }

  @Override
  public String getClusterName() {
    return this.rc.getClustername();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass()).add("is source", this.isSource()).add("end point name", this.getEndPointName())
        .add("hadoopfs config", this.rc).toString();
  }

  @Override
  public URI getFsURI() {
    return this.rc.getFsURI();
  }

  @Override
  public Path getDatasetPath(){
    return this.rc.getPath();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((rc == null) ? 0 : rc.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SourceHadoopFsEndPoint other = (SourceHadoopFsEndPoint) obj;
    if (rc == null) {
      if (other.rc != null)
        return false;
    } else if (!rc.equals(other.rc))
      return false;
    return true;
  }
}
