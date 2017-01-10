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

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import lombok.Getter;


/**
 * Used to encapsulate all the configuration for a hadoop file system replica
 * @author mitu
 *
 */
public class HadoopFsReplicaConfig {
  public static final String HDFS_COLO_KEY = "cluster.colo";
  public static final String HDFS_CLUSTERNAME_KEY = "cluster.name";
  public static final String HDFS_FILESYSTEM_URI_KEY = "cluster.FsURI";
  public static final String HDFS_PATH_KEY = "path";

  @Getter
  private final String colo;

  @Getter
  private final String clustername;

  @Getter
  private final URI fsURI;

  @Getter
  private final Path path;

  public HadoopFsReplicaConfig(Config config) {
    Preconditions.checkArgument(config.hasPath(HDFS_COLO_KEY));
    Preconditions.checkArgument(config.hasPath(HDFS_CLUSTERNAME_KEY));
    Preconditions.checkArgument(config.hasPath(HDFS_PATH_KEY));
    Preconditions.checkArgument(config.hasPath(HDFS_FILESYSTEM_URI_KEY));

    this.colo = config.getString(HDFS_COLO_KEY);
    this.clustername = config.getString(HDFS_CLUSTERNAME_KEY);
    this.path = new Path(config.getString(HDFS_PATH_KEY));
    try {
      this.fsURI = new URI(config.getString(HDFS_FILESYSTEM_URI_KEY));
    } catch (URISyntaxException e) {
      throw new RuntimeException("can not build URI based on " + config.getString(HDFS_FILESYSTEM_URI_KEY));
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass()).add("colo", this.colo).add("name", this.clustername)
        .add("FilesystemURI", this.fsURI).add("rootPath", this.path).toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((clustername == null) ? 0 : clustername.hashCode());
    result = prime * result + ((colo == null) ? 0 : colo.hashCode());
    result = prime * result + ((fsURI == null) ? 0 : fsURI.hashCode());
    result = prime * result + ((path == null) ? 0 : path.hashCode());
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
    HadoopFsReplicaConfig other = (HadoopFsReplicaConfig) obj;
    if (clustername == null) {
      if (other.clustername != null)
        return false;
    } else if (!clustername.equals(other.clustername))
      return false;
    if (colo == null) {
      if (other.colo != null)
        return false;
    } else if (!colo.equals(other.colo))
      return false;
    if (fsURI == null) {
      if (other.fsURI != null)
        return false;
    } else if (!fsURI.equals(other.fsURI))
      return false;
    if (path == null) {
      if (other.path != null)
        return false;
    } else if (!path.equals(other.path))
      return false;
    return true;
  }

}
