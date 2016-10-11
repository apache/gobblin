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

package gobblin.data.management.copy.replication;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.source.extractor.ComparableWatermark;
import gobblin.source.extractor.extract.LongWatermark;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ReplicaHadoopFsEndPoint extends HadoopFsEndPoint {
  public static final String WATERMARK_FILE = "_metadata";
  public static final String LATEST_TIMESTAMP = "latestTimestamp";

  @Getter
  private final HadoopFsReplicaConfig rc;

  @Getter
  private final String replicaName;
  
  private boolean initialized = false;
  private Optional<ComparableWatermark> cachedWatermark = Optional.absent();

  public ReplicaHadoopFsEndPoint(HadoopFsReplicaConfig rc, String replicaName) {
    Preconditions.checkArgument(!replicaName.equals(ReplicationConfiguration.REPLICATION_SOURCE),
        "replicaName can not be " + ReplicationConfiguration.REPLICATION_SOURCE);
    this.rc = rc;
    this.replicaName = replicaName;
    
  }

  @Override
  public Optional<ComparableWatermark> getWatermark() {
    if(this.initialized) {
      return this.cachedWatermark;
    }
    
    this.initialized = true;
    try {
      ComparableWatermark result;
      
      Path metaData = new Path(rc.getPath(), WATERMARK_FILE);
      FileSystem fs = FileSystem.get(rc.getFsURI(), new Configuration());
      if (fs.exists(metaData)) {
        try(FSDataInputStream fin = fs.open(metaData)){
          InputStreamReader reader = new InputStreamReader(fin, Charsets.UTF_8);
          Config c = ConfigFactory.parseReader(reader);
          result = new LongWatermark(c.getLong(LATEST_TIMESTAMP));
        }
        this.cachedWatermark = Optional.of(result);
        return this.cachedWatermark;
      }
      // for replica, can not use the file time stamp as that is different with original source time stamp

      return this.cachedWatermark;
    } catch (IOException e) {
      log.warn("Can not find " + WATERMARK_FILE + " for replica " + this);
      return this.cachedWatermark;
    }
  }

  @Override
  public boolean isSource() {
    return false;
  }

  @Override
  public String getEndPointName() {
    return this.replicaName;
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
    result = prime * result + ((replicaName == null) ? 0 : replicaName.hashCode());
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
    ReplicaHadoopFsEndPoint other = (ReplicaHadoopFsEndPoint) obj;
    if (rc == null) {
      if (other.rc != null)
        return false;
    } else if (!rc.equals(other.rc))
      return false;
    if (replicaName == null) {
      if (other.replicaName != null)
        return false;
    } else if (!replicaName.equals(other.replicaName))
      return false;
    return true;
  }
}
