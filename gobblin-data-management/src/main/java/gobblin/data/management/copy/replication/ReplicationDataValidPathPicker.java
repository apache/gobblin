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
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import gobblin.data.management.version.TimestampedDatasetVersion;
import gobblin.data.management.version.finder.AbstractDatasetVersionFinder;
import gobblin.data.management.version.finder.DateTimeDatasetVersionFinder;
import gobblin.data.management.version.finder.GlobModTimeDatasetVersionFinder;
import gobblin.dataset.FileSystemDataset;

/**
 * Used to pick the valid Paths for data replication based on {@link #ReplicationDataRetentionCategory.Type}
 * @author mitu
 *
 */
public class ReplicationDataValidPathPicker {

  public static Collection<Path> getValidPaths(HadoopFsEndPoint hadoopFsEndPoint) throws IOException{
    ReplicationDataRetentionCategory rdc = hadoopFsEndPoint.getReplicationDataRetentionCategory();
    if(rdc.getType() == ReplicationDataRetentionCategory.Type.SYNC){
      return Lists.newArrayList(hadoopFsEndPoint.getDatasetPath());
    }

    Preconditions.checkArgument(rdc.getFiniteInstance().isPresent());
    int finiteInstance = rdc.getFiniteInstance().get();

    FileSystemDataset tmpDataset = new HadoopFsEndPointDataset(hadoopFsEndPoint);
    FileSystem theFs = FileSystem.get(hadoopFsEndPoint.getFsURI(), new Configuration());
    
    AbstractDatasetVersionFinder<TimestampedDatasetVersion> absfinder = null;
    Properties p = new Properties();
    switch(rdc.getType()){
      case FINITE_SNAPSHOT: 
        absfinder = new GlobModTimeDatasetVersionFinder(theFs, p);
        break;
      case FINITE_DAILY_PARTITION:
        p.setProperty(DateTimeDatasetVersionFinder.DATE_TIME_PATTERN_KEY, "yyyy/MM/dd");
        absfinder = new DateTimeDatasetVersionFinder(theFs, p);
        break;
      case  FINITE_HOURLY_PARTITION: 
        p.setProperty(DateTimeDatasetVersionFinder.DATE_TIME_PATTERN_KEY, "yyyy/MM/dd/hh");
        absfinder = new DateTimeDatasetVersionFinder(theFs, p);
        break;
      default:
        throw new IllegalArgumentException("Unsupported type " + rdc.getType());
    }
    
    List<TimestampedDatasetVersion> versions = 
        Ordering.natural().reverse().sortedCopy(absfinder.findDatasetVersions(tmpDataset));

    versions = versions.subList(0, Math.min(finiteInstance, versions.size()));

    return Lists.transform(versions, new Function<TimestampedDatasetVersion, Path>() {
      @Override
      public Path apply(TimestampedDatasetVersion input) {
        return input.getPath();
      }
    });
  }
}
