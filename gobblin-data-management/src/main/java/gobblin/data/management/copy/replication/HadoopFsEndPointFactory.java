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

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.annotation.Alias;

@Alias(value="HadoopFsEndPointFactory")
public class HadoopFsEndPointFactory implements EndPointFactory {
  public static final String HADOOP_FS_CONFIG_KEY = "hadoopfs";

  @Override
  public HadoopFsEndPoint buildSource(Config sourceConfig) {
    Preconditions.checkArgument(sourceConfig.hasPath(HADOOP_FS_CONFIG_KEY),
        "missing required config entery " + HADOOP_FS_CONFIG_KEY);

    return new SourceHadoopFsEndPoint(new HadoopFsReplicaConfig(sourceConfig.getConfig(HADOOP_FS_CONFIG_KEY)));
  }

  @Override
  public HadoopFsEndPoint buildReplica(Config replicaConfig, String replicaName) {
    Preconditions.checkArgument(replicaConfig.hasPath(HADOOP_FS_CONFIG_KEY),
        "missing required config entery " + HADOOP_FS_CONFIG_KEY);

    return new ReplicaHadoopFsEndPoint(new HadoopFsReplicaConfig(replicaConfig.getConfig(HADOOP_FS_CONFIG_KEY)),
        replicaName);
  }
}
