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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.jetty.util.log.Log;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.typesafe.config.ConfigFactory;

import gobblin.data.management.version.TimestampedDatasetVersion;
import gobblin.data.management.version.finder.AbstractDatasetVersionFinder;
import gobblin.data.management.version.finder.DateTimeDatasetVersionFinder;
import gobblin.data.management.version.finder.GlobModTimeDatasetVersionFinder;
import gobblin.data.management.version.finder.ModDateTimeDatasetVersionFinder;
import gobblin.data.management.version.finder.UnixTimestampVersionFinder;
import gobblin.dataset.FileSystemDataset;

public class ReplicationDataValidPathPicker {

  /**
   * Under root Path, based on the modification time, select the latest snapshot directories, defined by finiteInstance. 
   */
  private static Collection<Path> getValidSnapshotPaths(FileSystem fs, Path root, int finiteInstance) throws IOException{
    Preconditions.checkArgument(finiteInstance>0);
    FileStatus[] fileStatus = fs.listStatus(root); 

    PriorityQueue<FileStatus> pq = new PriorityQueue<FileStatus>(new Comparator<FileStatus>(){
      @Override
      public int compare(FileStatus o1, FileStatus o2) {
        if (o1.getModificationTime()>o2.getModificationTime()) return 1;
        else if (o1.getModificationTime() ==o2.getModificationTime()) return 0;
        else return -1;
      }
    });

    for(FileStatus f: fileStatus){
      if(!f.isDirectory()) continue;

      if(pq.size()<finiteInstance){
        pq.add(f);
      }
      else{
        if(f.getModificationTime()>pq.peek().getModificationTime()){
          pq.poll();
          pq.add(f);
        }
      }
    }

    return Collections2.transform(pq, new Function<FileStatus, Path>() {
      @Override
      public Path apply(FileStatus t) {
        return t.getPath();
      }
    });
  }

  /**
   * Under root Path, the sub directory should have YYYY/MM/DD format
   * Based on lookbackDays parameter, pick the corresponding directory
   */
  private static Collection<Path> getValidDailyPartitionPaths(FileSystem fs, Path root, int lookbackDays) throws IOException{
    DateTimeZone PST = DateTimeZone.forID("America/Los_Angeles");
    DateTime time = new DateTime(PST);
    DateTime oldDate = time.minusDays(lookbackDays);
    FileStatus[] fileStatus = fs.globStatus(new Path(root, "*/*/*"));
    List<Path> validPaths = new ArrayList<>();
    for(FileStatus f: fileStatus){
      if(!f.isDirectory()) continue;
      String day = f.getPath().getName();
      String month = f.getPath().getParent().getName();
      String year = f.getPath().getParent().getParent().getName();

      try{
        DateTime dirTime = new DateTime(Integer.parseInt(year), Integer.parseInt(month), Integer.parseInt(day), 0, 0, 0, 0, PST);
        if(dirTime.getMillis() > oldDate.getMillis()){
          validPaths.add(f.getPath());
        }
      }
      catch(Exception ne){
        // ignored
      }
    }

    return validPaths;
  }

  /**
   * Under root Path, the sub directory should have YYYY/MM/DD/HH format
   * Based on lookbackDays parameter, pick the corresponding directory
   */
  private static Collection<Path> getValidHourlyPartitionPaths(FileSystem fs, Path root, int lookbackHours) throws IOException{
    DateTimeZone PST = DateTimeZone.forID("America/Los_Angeles");
    DateTime time = new DateTime(PST);
    DateTime oldDate = time.minusHours(lookbackHours);
    FileStatus[] fileStatus = fs.globStatus(new Path(root, "*/*/*/*"));
    List<Path> validPaths = new ArrayList<>();
    for(FileStatus f: fileStatus){
      if(!f.isDirectory()) continue;
      String hour  = f.getPath().getName();
      String day   = f.getPath().getParent().getName();
      String month = f.getPath().getParent().getParent().getName();
      String year  = f.getPath().getParent().getParent().getParent().getName();

      try{
        DateTime dirTime = new DateTime(Integer.parseInt(year), Integer.parseInt(month), Integer.parseInt(day), Integer.parseInt(hour), 0, 0, 0, PST);
        if(dirTime.getMillis() > oldDate.getMillis()){
          validPaths.add(f.getPath());
        }
      }
      catch(Exception ne){
        // ignored
      }
    }

    return validPaths;
  }

  /**
   * @param fs The {@link FileSystem} where the root Path located
   * @param root The root Path
   * @param rdc To specify data is Sync or Snapshot or Append
   * @return The valid Paths for Replication
   * @throws IOException
   */
  public static Collection<Path> getValidPaths(FileSystem fs, Path root, ReplicationDataRetentionCategory rdc) throws IOException{
    if(rdc.getType() == ReplicationDataRetentionCategory.Type.SYNC){
      return Lists.newArrayList(root);
    }

    Preconditions.checkArgument(rdc.getFiniteInstance().isPresent());
    int finiteInstance = rdc.getFiniteInstance().get();

    // for snapshots, get the latest dirs
    if(rdc.getType() == ReplicationDataRetentionCategory.Type.FINITE_SNAPSHOT){
      return getValidSnapshotPaths(fs, root, finiteInstance);
    }
    // format is YYYY/MM/DD
    else if(rdc.getType() == ReplicationDataRetentionCategory.Type.FINITE_DAILY_PARTITION){
      return getValidDailyPartitionPaths(fs, root, finiteInstance);
    }
    // format is YYYY/MM/DD/HH
    else if(rdc.getType() == ReplicationDataRetentionCategory.Type.FINITE_HOURLY_PARTITION){
      return getValidHourlyPartitionPaths(fs, root, finiteInstance);
    }
    else 
      throw new IllegalArgumentException("Unsupported type " + rdc.getType());
  }

  public static Collection<Path> getValidPaths(HadoopFsEndPoint hadoopFsEndPoint) throws IOException{
    ReplicationDataRetentionCategory rdc = hadoopFsEndPoint.getReplicationDataRetentionCategory();
    if(rdc.getType() == ReplicationDataRetentionCategory.Type.SYNC){
      return Lists.newArrayList(hadoopFsEndPoint.getDatasetPath());
    }

    Preconditions.checkArgument(rdc.getFiniteInstance().isPresent());
    int finiteInstance = rdc.getFiniteInstance().get();

    FileSystemDataset tmpDataset = new HadoopFsEndPointDataset(hadoopFsEndPoint);
    FileSystem theFs = FileSystem.get(hadoopFsEndPoint.getFsURI(), new Configuration());
    
    AbstractDatasetVersionFinder absfinder;
    switch(rdc.getType()){
      case FINITE_SNAPSHOT: 
        absfinder = new GlobModTimeDatasetVersionFinder(theFs, ConfigFactory.empty());
      case FINITE_DAILY_PARTITION:
        
       
        default:
          
    }

    if(rdc.getType() == ReplicationDataRetentionCategory.Type.FINITE_SNAPSHOT){
      // for snapshots, get the latest dirs based on the modification time of FileStatus
      GlobModTimeDatasetVersionFinder finder = 
          new GlobModTimeDatasetVersionFinder(theFs, ConfigFactory.empty());
      List<TimestampedDatasetVersion> versions = 
          Ordering.natural().reverse().sortedCopy(finder.findDatasetVersions(tmpDataset));

      //
      //      for(TimestampedDatasetVersion v: versions){
      //        System.out.println("BBB " + v);
      //      }
      //      
      //      System.out.println("BBB instance num is " + finiteInstance);

      versions = versions.subList(0, Math.min(finiteInstance, versions.size()));

      return Lists.transform(versions, new Function<TimestampedDatasetVersion, Path>() {
        @Override
        public Path apply(TimestampedDatasetVersion input) {
          return input.getPath();
        }
      });
    }
    // format is YYYY/MM/DD
    else if(rdc.getType() == ReplicationDataRetentionCategory.Type.FINITE_DAILY_PARTITION){
      Properties p = new Properties();
      p.setProperty(DateTimeDatasetVersionFinder.DATE_TIME_PATTERN_KEY, "yyyy/MM/dd");
      DateTimeDatasetVersionFinder finder = new DateTimeDatasetVersionFinder(theFs, p);
      List<TimestampedDatasetVersion> versions = 
          Ordering.natural().reverse().sortedCopy(finder.findDatasetVersions(tmpDataset));

      for(TimestampedDatasetVersion v: versions){
        System.out.println("CCC " + v);
      }

      System.out.println("CCC instance num is " + finiteInstance);

      versions = versions.subList(0, Math.min(finiteInstance, versions.size()));

      return Lists.transform(versions, new Function<TimestampedDatasetVersion, Path>() {
        @Override
        public Path apply(TimestampedDatasetVersion input) {
          return input.getPath();
        }
      });
    }
    //    // format is YYYY/MM/DD/HH
    //    else if(rdc.getType() == ReplicationDataRetentionCategory.Type.FINITE_HOURLY_PARTITION){
    //       
    //    }
    else 
      throw new IllegalArgumentException("Unsupported type " + rdc.getType());
  }

}
