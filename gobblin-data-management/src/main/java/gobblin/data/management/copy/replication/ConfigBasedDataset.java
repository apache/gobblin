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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;

import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyableDataset;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.entities.PostPublishStep;
import gobblin.data.management.copy.entities.PrePublishStep;
import gobblin.data.management.dataset.DatasetUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.PathUtils;
import gobblin.util.commit.DeleteFileCommitStep;
import lombok.extern.slf4j.Slf4j;


/**
 * Extends {@link CopyableDataset} to represent data replication dataset based on {@link Config}
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
  private String datasetURN;
  private boolean watermarkEnabled;

  public ConfigBasedDataset(ReplicationConfiguration rc, Properties props, CopyRoute copyRoute) {
    this.props = props;
    this.copyRoute = copyRoute;
    this.rc = rc;
    this.watermarkEnabled = Boolean.parseBoolean
        (this.props.getProperty(ConfigBasedDatasetsFinder.WATERMARK_ENABLE, "true"));
    calculateDatasetURN();
  }

  private void calculateDatasetURN(){
    EndPoint e = this.copyRoute.getCopyTo();
    if (e instanceof HadoopFsEndPoint) {
      HadoopFsEndPoint copyTo = (HadoopFsEndPoint) e;
      Configuration conf = HadoopUtils.newConfiguration();
      try {
        FileSystem copyToFs = FileSystem.get(copyTo.getFsURI(), conf);
        this.datasetURN = copyToFs.makeQualified(copyTo.getDatasetPath()).toString();
      } catch (IOException e1) {
        // ignored
      }
    } else {
      this.datasetURN = e.toString();
    }
  }

  @Override
  public String datasetURN() {
    return this.datasetURN;
  }

  @Override
  public Collection<? extends CopyEntity> getCopyableFiles(FileSystem targetFs, CopyConfiguration copyConfiguration)
      throws IOException {
    List<CopyEntity> copyableFiles = Lists.newArrayList();
    EndPoint copyFromRaw = copyRoute.getCopyFrom();
    EndPoint copyToRaw = copyRoute.getCopyTo();
    if (!(copyFromRaw instanceof HadoopFsEndPoint && copyToRaw instanceof HadoopFsEndPoint)) {
      log.warn("Currently only handle the Hadoop Fs EndPoint replication");
      return copyableFiles;
    }

    if (this.watermarkEnabled) {
      if ((!copyFromRaw.getWatermark().isPresent() && copyToRaw.getWatermark().isPresent()) || (
          copyFromRaw.getWatermark().isPresent() && copyToRaw.getWatermark().isPresent()
              && copyFromRaw.getWatermark().get().compareTo(copyToRaw.getWatermark().get()) <= 0)) {
        log.info(
            "No need to copy as destination watermark >= source watermark with source watermark {}, for dataset with metadata {}",
            copyFromRaw.getWatermark().isPresent() ? copyFromRaw.getWatermark().get().toJson() : "N/A",
            this.rc.getMetaData());
        return copyableFiles;
      }
    }

    HadoopFsEndPoint copyFrom = (HadoopFsEndPoint) copyFromRaw;
    HadoopFsEndPoint copyTo = (HadoopFsEndPoint) copyToRaw;
    Configuration conf = HadoopUtils.newConfiguration();
    FileSystem copyFromFs = FileSystem.get(copyFrom.getFsURI(), conf);
    FileSystem copyToFs = FileSystem.get(copyTo.getFsURI(), conf);

    Collection<FileStatus> allFilesInSource = copyFrom.getFiles();
    Collection<FileStatus> allFilesInTarget = copyTo.getFiles();

    final PathFilter pathFilter = DatasetUtils.instantiatePathFilter(this.props);
    Predicate<FileStatus> predicate = new Predicate<FileStatus>() {
      @Override
      public boolean apply(FileStatus input) {
        return pathFilter.accept(input.getPath());
      }
    };

    Set<FileStatus> copyFromFileStatuses = Sets.newHashSet(Collections2.filter(allFilesInSource, predicate));
    Map<Path, FileStatus> copyToFileMap = Maps.newHashMap();
    for(FileStatus f: allFilesInTarget){
      if(pathFilter.accept(f.getPath())){
        copyToFileMap.put(PathUtils.getPathWithoutSchemeAndAuthority(f.getPath()), f);
      }
    }

    Collection<Path> deletedPaths = Lists.newArrayList();

    boolean watermarkMetadataCopied = false;

    boolean deleteTargetIfNotExistOnSource = rc.isDeleteTargetIfNotExistOnSource();

    for (FileStatus originFileStatus : copyFromFileStatuses) {
      Path relative = PathUtils.relativizePath(PathUtils.getPathWithoutSchemeAndAuthority(originFileStatus.getPath()),
          PathUtils.getPathWithoutSchemeAndAuthority(copyFrom.getDatasetPath()));
      // construct the new path in the target file system
      Path newPath = new Path(copyTo.getDatasetPath(), relative);

      if (relative.toString().equals(ReplicaHadoopFsEndPoint.WATERMARK_FILE)) {
        watermarkMetadataCopied = true;
      }

      // skip copy same file
      if (copyToFileMap.containsKey(newPath) && copyToFileMap.get(newPath).getLen() == originFileStatus.getLen()
          && copyToFileMap.get(newPath).getModificationTime() > originFileStatus.getModificationTime()) {
        log.debug("Copy from timestamp older than copy to timestamp, skipped copy {} for dataset with metadata {}",
            originFileStatus.getPath(), this.rc.getMetaData());
      } else {
        // need to remove those files in the target File System
        if (copyToFileMap.containsKey(newPath)) {
          deletedPaths.add(newPath);
        }

        copyableFiles
            .add(CopyableFile.fromOriginAndDestination(copyFromFs, originFileStatus, copyToFs.makeQualified(newPath), copyConfiguration)
                .fileSet(PathUtils.getPathWithoutSchemeAndAuthority(copyTo.getDatasetPath()).toString()).build());

      }

      // clean up already checked paths
      copyToFileMap.remove(newPath);
    }

    // delete the paths on target directory if NOT exists on source
    if (deleteTargetIfNotExistOnSource) {
      deletedPaths.addAll(copyToFileMap.keySet());
    }

    // delete old files first
    if (!deletedPaths.isEmpty()) {
      DeleteFileCommitStep deleteCommitStep = DeleteFileCommitStep.fromPaths(copyToFs, deletedPaths, this.props);
      copyableFiles.add(new PrePublishStep(copyTo.getDatasetPath().toString(), Maps.<String, String> newHashMap(),
          deleteCommitStep, 0));
    }

    // generate the watermark file even if watermark checking is disabled. Make sure it can come into functional once disired.
    if ((!watermarkMetadataCopied) && copyFrom.getWatermark().isPresent()) {
      copyableFiles.add(new PostPublishStep(copyTo.getDatasetPath().toString(), Maps.<String, String> newHashMap(),
          new WatermarkMetadataGenerationCommitStep(copyTo.getFsURI().toString(), copyTo.getDatasetPath(),
              copyFrom.getWatermark().get()),
          1));
    }
    return copyableFiles;
  }

}
