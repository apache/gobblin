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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyableDataset;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.RecursivePathFinder;
import gobblin.data.management.copy.entities.PostPublishStep;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.util.HadoopUtils;
import gobblin.util.PathUtils;
import lombok.extern.slf4j.Slf4j;


/**
 * Extends {@link CopableDataset} to represent data replication dataset based on {@link Config}
 * 
 * Detail logics
 * <ul>
 *  <li>Picked the preferred topology
 *  <li>Based on current running cluster and CopyMode (push or pull) pick the routes
 *  <li>Based on optimization policy to pick the CopyFrom and CopyTo pair
 *  <li>Generated the CopyEntity based on CopyFrom and CopyTo pair
 * </ul>
 * @author mitu
 *
 */

@Slf4j
public class ConfigBasedDataset implements CopyableDataset {

  private final Properties props;
  private final CopyRoute copyRoute;
  private final ReplicationConfiguration rc;

  public ConfigBasedDataset(ReplicationConfiguration rc, Properties props, CopyRoute copyRoute) {
    this.props = props;
    this.copyRoute = copyRoute;
    this.rc = rc;
  }

  @Override
  public String datasetURN() {
    EndPoint e = this.copyRoute.getCopyTo();
    if(e instanceof HadoopFsEndPoint){
      return ((HadoopFsEndPoint)e).getDatasetPath().toString();
    }
    
    return e.toString();
  }

  @Override
  public Collection<? extends CopyEntity> getCopyableFiles(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {
    List<CopyEntity> copyableFiles = Lists.newArrayList();
    EndPoint copyFromRaw = copyRoute.getCopyFrom();
    EndPoint copyToRaw = copyRoute.getCopyTo();
    if (!(copyFromRaw instanceof HadoopFsEndPoint && copyToRaw instanceof HadoopFsEndPoint)) {
      log.warn("Currently only handle the Hadoop Fs EndPoint replication");
      return copyableFiles;
    }

    if ((!copyFromRaw.getWatermark().isPresent() && copyToRaw.getWatermark().isPresent())
        || (copyFromRaw.getWatermark().isPresent() && copyToRaw.getWatermark().isPresent()
            && copyFromRaw.getWatermark().get().compareTo(copyToRaw.getWatermark().get()) <= 0)) {
      log.info(String.format(
          "No need to copy as destination watermark >= source watermark with source watermark %s, for dataset with metadata %s",
          copyFromRaw.getWatermark().isPresent() ? copyFromRaw.getWatermark().get().toJson() : "N/A",
          this.rc.getMetaData()));
      return copyableFiles;
    }

    HadoopFsEndPoint copyFrom = (HadoopFsEndPoint) copyFromRaw;
    Configuration conf = HadoopUtils.newConfiguration();
    FileSystem copyFromFs = FileSystem.get(copyFrom.getFsURI(), conf);
    RecursivePathFinder finder = new RecursivePathFinder(copyFromFs, copyFrom.getDatasetPath(), this.props);
    Set<FileStatus> copyFromFileStatuses = finder.getPaths(false);
    
    HadoopFsEndPoint copyTo = (HadoopFsEndPoint) copyToRaw;
    FileSystem copyToFs = FileSystem.get(copyTo.getFsURI(), conf);
    finder = new RecursivePathFinder(copyToFs, copyTo.getDatasetPath(), this.props);
    Set<FileStatus> copyToFileStatuses = finder.getPaths(false);
    Map<Path, FileStatus> copyToFileMap = new HashMap<>();
    for (FileStatus f : copyToFileStatuses) {
      copyToFileMap.put(PathUtils.getPathWithoutSchemeAndAuthority(f.getPath()), f);
    }

    for (FileStatus originFileStatus : copyFromFileStatuses) {
      // construct the new path in the target file system
      Path newPath = new Path(copyTo.getDatasetPath(),
          PathUtils.relativizePath(PathUtils.getPathWithoutSchemeAndAuthority(originFileStatus.getPath()),
              PathUtils.getPathWithoutSchemeAndAuthority(copyFrom.getDatasetPath())));

      if (copyToFileMap.containsKey(newPath) && copyToFileMap.get(newPath).getLen() == originFileStatus.getLen()
          && copyToFileMap.get(newPath).getModificationTime() > originFileStatus.getModificationTime()) {
        log.debug(String.format(
            "Copy from timestamp older than copy to timestamp, skipped copy from %s for dataset with metadata %s",
            originFileStatus.getPath(), this.rc.getMetaData()));
      } else {
        copyableFiles.add(CopyableFile.fromOriginAndDestination(copyFromFs, originFileStatus, newPath, configuration)
            .fileSet(PathUtils.getPathWithoutSchemeAndAuthority(copyTo.getDatasetPath()).toString()).build());
      }
    }

    if (copyFrom.getWatermark().isPresent()) {
      LongWatermark tmp = (LongWatermark) (copyFrom.getWatermark().get());
      copyableFiles.add(new PostPublishStep(copyTo.getDatasetPath().toString(), Maps.<String, String> newHashMap(),
          new WatermarkMetadataGenerationCommitStep(copyTo.getFsURI().toString(), copyTo.getDatasetPath(),
              tmp.getValue()),
          0));
    }

    return copyableFiles;
  }

}
