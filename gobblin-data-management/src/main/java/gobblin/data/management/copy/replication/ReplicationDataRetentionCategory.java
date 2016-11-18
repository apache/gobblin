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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * Specify the replication data type: sync, finite_snapshot,  finite_daily_partition or finite_hourly_partition
 * <ul>
 *  <li>sync: all files in source need to be copied to target
 *  <li>finite_daily_partition : only limited daily partitions in source need to be copied to target
 *  <li>finite_daily_partition : only limited hourly partitions in source need to be copied to target
 *  <li>finite_snapshot: only limited snapshot directories in source need to be copied to target
 * </ul>
 * @author mitu
 *
 */
@AllArgsConstructor
public class ReplicationDataRetentionCategory {
  enum Type{
    SYNC("sync"),
    FINITE_DAILY_PARTITION("finite_daily_partition"),
    FINITE_HOURLY_PARTITION("finite_hourly_partition"),
    FINITE_SNAPSHOT("finite_snapshot");
    
    private final String name;
    Type(String name){
      this.name = name;
    }
    
    @Override
    public String toString() {
      return this.name;
    }
    
    public static Type forName(String name) {
      return Type.valueOf(name.toUpperCase());
    }
  };
  
  @Getter
  private Type type;
  
  @Getter
  private Optional<Integer> finiteInstance;

  public static ReplicationDataRetentionCategory getReplicationCopyMode(Config config) {
    ReplicationDataRetentionCategory.Type type = config.hasPath(ReplicationConfiguration.REPLICATION_DATA_CATETORY_TYPE)
        ? ReplicationDataRetentionCategory.Type.forName(config.getString(ReplicationConfiguration.REPLICATION_DATA_CATETORY_TYPE))
        : ReplicationDataRetentionCategory.Type.SYNC;
        
    if(type != ReplicationDataRetentionCategory.Type.SYNC){
      Preconditions.checkArgument(config.hasPath(ReplicationConfiguration.REPLICATION_DATA_FINITE_INSTANCE));
      int count = config.getInt(ReplicationConfiguration.REPLICATION_DATA_FINITE_INSTANCE);
      return new ReplicationDataRetentionCategory(type, Optional.of(count));
    }
    else{
      return new ReplicationDataRetentionCategory(type, Optional.<Integer>absent());
    }
  }

}
