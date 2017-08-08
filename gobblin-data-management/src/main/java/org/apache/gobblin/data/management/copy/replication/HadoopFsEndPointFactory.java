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

package org.apache.gobblin.data.management.copy.replication;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alias;

@Alias(value="HadoopFsEndPointFactory")
public class HadoopFsEndPointFactory implements EndPointFactory {
  public static final String HADOOP_FS_CONFIG_KEY = "hadoopfs";

  @Override
  public HadoopFsEndPoint buildSource(Config sourceConfig, Config selectionConfig) {
    Preconditions.checkArgument(sourceConfig.hasPath(HADOOP_FS_CONFIG_KEY),
        "missing required config entery " + HADOOP_FS_CONFIG_KEY);

    return new SourceHadoopFsEndPoint(new HadoopFsReplicaConfig(sourceConfig.getConfig(HADOOP_FS_CONFIG_KEY)), selectionConfig);
  }

  @Override
  public HadoopFsEndPoint buildReplica(Config replicaConfig, String replicaName, Config selectionConfig) {
    Preconditions.checkArgument(replicaConfig.hasPath(HADOOP_FS_CONFIG_KEY),
        "missing required config entery " + HADOOP_FS_CONFIG_KEY);

    return new ReplicaHadoopFsEndPoint(new HadoopFsReplicaConfig(replicaConfig.getConfig(HADOOP_FS_CONFIG_KEY)),
        replicaName, selectionConfig);
  }
}
