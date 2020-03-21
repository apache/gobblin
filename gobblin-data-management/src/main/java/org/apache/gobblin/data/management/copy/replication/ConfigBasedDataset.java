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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.gobblin.util.filesystem.ModTimeDataFileVersionStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyableDataset;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.entities.PostPublishStep;
import org.apache.gobblin.data.management.copy.entities.PrePublishStep;
import org.apache.gobblin.data.management.dataset.DatasetUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.commit.DeleteFileCommitStep;
import org.apache.gobblin.util.filesystem.DataFileVersionStrategy;

import lombok.Getter;
import lombok.Setter;
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
  private final PathFilter pathFilter;
  private final Optional<DataFileVersionStrategy> srcDataFileVersionStrategy;
  private final Optional<DataFileVersionStrategy> dstDataFileVersionStrategy;
  @Setter @Getter
  protected String expectedSchema = null;

  //Apply filter to directories
  private final boolean applyFilterToDirectories;

  //Version strategy from config store
  private Optional<String> versionStrategyFromCS = Optional.absent();

  public ConfigBasedDataset(ReplicationConfiguration rc, Properties props, CopyRoute copyRoute) {
    this.props = props;
    this.copyRoute = copyRoute;
    this.rc = rc;
    calculateDatasetURN();
    this.watermarkEnabled =
        Boolean.parseBoolean(this.props.getProperty(ConfigBasedDatasetsFinder.WATERMARK_ENABLE, "true"));
    this.pathFilter = DatasetUtils.instantiatePathFilter(this.props);
    this.applyFilterToDirectories =
        Boolean.parseBoolean(this.props.getProperty(CopyConfiguration.APPLY_FILTER_TO_DIRECTORIES, "false"));
    this.srcDataFileVersionStrategy = initDataFileVersionStrategy(this.copyRoute.getCopyFrom(), rc, props);
    this.dstDataFileVersionStrategy = initDataFileVersionStrategy(this.copyRoute.getCopyTo(), rc, props);
  }

  public ConfigBasedDataset(ReplicationConfiguration rc, Properties props, CopyRoute copyRoute, String datasetURN) {
    this.props = props;
    this.copyRoute = copyRoute;
    this.rc = rc;
    this.datasetURN = datasetURN;
    this.pathFilter = DatasetUtils.instantiatePathFilter(this.props);
    this.applyFilterToDirectories =
        Boolean.parseBoolean(this.props.getProperty(CopyConfiguration.APPLY_FILTER_TO_DIRECTORIES, "false"));
    this.srcDataFileVersionStrategy = initDataFileVersionStrategy(this.copyRoute.getCopyFrom(), rc, props);
    this.dstDataFileVersionStrategy = initDataFileVersionStrategy(this.copyRoute.getCopyTo(), rc, props);
  }

  /**
   * Get the version strategy that can retrieve the data file version from the end point.
   *
   * @return the version strategy. Empty value when the version is not supported for this end point.
   */
  private Optional<DataFileVersionStrategy> initDataFileVersionStrategy(EndPoint endPoint, ReplicationConfiguration rc, Properties props) {

    // rc is the dataset config???
    if (!(endPoint instanceof HadoopFsEndPoint)) {
      log.warn("Data file version currently only handle the Hadoop Fs EndPoint replication");
      return Optional.absent();
    }
    Configuration conf = HadoopUtils.newConfiguration();
    try {
      HadoopFsEndPoint hEndpoint = (HadoopFsEndPoint) endPoint;
      FileSystem fs = FileSystem.get(hEndpoint.getFsURI(), conf);

      // If configStore doesn't contain the strategy, check from job properties.
      // If no strategy is found, default to the modification time strategy.
      this.versionStrategyFromCS = rc.getVersionStrategyFromConfigStore();
      String nonEmptyStrategy = versionStrategyFromCS.isPresent()? versionStrategyFromCS.get() :
          props.getProperty(DataFileVersionStrategy.DATA_FILE_VERSION_STRATEGY_KEY, DataFileVersionStrategy.DEFAULT_DATA_FILE_VERSION_STRATEGY);
      Config versionStrategyConfig = ConfigFactory.parseMap(ImmutableMap.of(
          DataFileVersionStrategy.DATA_FILE_VERSION_STRATEGY_KEY, nonEmptyStrategy));

      DataFileVersionStrategy strategy = DataFileVersionStrategy.instantiateDataFileVersionStrategy(fs, versionStrategyConfig);
      log.debug("{} has version strategy {}", hEndpoint.getClusterName(), strategy.getClass().getName());
      return Optional.of(strategy);
    } catch (IOException e) {
      log.error("Version strategy cannot be created due to {}", e);
      return Optional.absent();
    }
  }

  private void calculateDatasetURN() {
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

  public boolean schemaCheckEnabled() { return this.rc.isSchemaCheckEnabled(); }

  @Override
  public String datasetURN() {
    return this.datasetURN;
  }

  @Override
  public Collection<? extends CopyEntity> getCopyableFiles(FileSystem targetFs, CopyConfiguration copyConfiguration)
      throws IOException {
    boolean enforceFileSizeMatch = this.rc.getEnforceFileSizeMatchFromConfigStore().isPresent()?
        this.rc.getEnforceFileSizeMatchFromConfigStore().get() :
        copyConfiguration.isEnforceFileLengthMatch();

    List<CopyEntity> copyableFiles = Lists.newArrayList();
    EndPoint copyFromRaw = copyRoute.getCopyFrom();
    EndPoint copyToRaw = copyRoute.getCopyTo();
    if (!(copyFromRaw instanceof HadoopFsEndPoint && copyToRaw instanceof HadoopFsEndPoint)) {
      log.warn("Currently only handle the Hadoop Fs EndPoint replication");
      return copyableFiles;
    }

    if (!this.srcDataFileVersionStrategy.isPresent() || !this.dstDataFileVersionStrategy.isPresent()) {
      log.warn("Version strategy doesn't exist, cannot handle copy");
      return copyableFiles;
    }

    if (!this.srcDataFileVersionStrategy.get().getClass().getName()
        .equals(this.dstDataFileVersionStrategy.get().getClass().getName())) {
      log.warn("Version strategy src: {} and dst: {} doesn't match, cannot handle copy.",
          this.srcDataFileVersionStrategy.get().getClass().getName(),
          this.dstDataFileVersionStrategy.get().getClass().getName());
      return copyableFiles;
    }

    //For {@link HadoopFsEndPoint}s, set pathfilter and applyFilterToDirectories
    HadoopFsEndPoint copyFrom = (HadoopFsEndPoint) copyFromRaw;
    HadoopFsEndPoint copyTo = (HadoopFsEndPoint) copyToRaw;
    copyFrom.setPathFilter(pathFilter);
    copyFrom.setApplyFilterToDirectories(applyFilterToDirectories);
    copyTo.setPathFilter(pathFilter);
    copyTo.setApplyFilterToDirectories(applyFilterToDirectories);

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

    Configuration conf = HadoopUtils.newConfiguration();
    FileSystem copyFromFs = FileSystem.get(copyFrom.getFsURI(), conf);
    FileSystem copyToFs = FileSystem.get(copyTo.getFsURI(), conf);

    Collection<FileStatus> allFilesInSource = copyFrom.getFiles();
    Collection<FileStatus> allFilesInTarget = copyTo.getFiles();

    Set<FileStatus> copyFromFileStatuses = Sets.newHashSet(allFilesInSource);
    Map<Path, FileStatus> copyToFileMap = Maps.newHashMap();
    for (FileStatus f : allFilesInTarget) {
      copyToFileMap.put(PathUtils.getPathWithoutSchemeAndAuthority(f.getPath()), f);
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


      boolean shouldCopy = true;
      // Can optimize by using the mod time that has already been fetched
      boolean useDirectGetModTime = this.srcDataFileVersionStrategy.isPresent()
          && this.srcDataFileVersionStrategy.get().getClass().getName().equals(
              ModTimeDataFileVersionStrategy.class.getName());

      if (copyToFileMap.containsKey(newPath)) {
        Comparable srcVer = useDirectGetModTime ? originFileStatus.getModificationTime() :
            this.srcDataFileVersionStrategy.get().getVersion(originFileStatus.getPath());
        Comparable dstVer = useDirectGetModTime ? copyToFileMap.get(newPath).getModificationTime() :
            this.dstDataFileVersionStrategy.get().getVersion(copyToFileMap.get(newPath).getPath());

        // destination has higher version, skip the copy
        if (srcVer.compareTo(dstVer) <= 0) {
          if (!enforceFileSizeMatch || copyToFileMap.get(newPath).getLen() == originFileStatus.getLen()) {
            log.debug("Copy from src {} (v:{}) to dst {} (v:{}) can be skipped.",
                originFileStatus.getPath(), srcVer, copyToFileMap.get(newPath).getPath(), dstVer);
            shouldCopy = false;
          } else {
            log.debug("Copy from src {} (v:{}) to dst {} (v:{}) can not be skipped due to unmatched file length.",
                originFileStatus.getPath(), srcVer, copyToFileMap.get(newPath).getPath(), dstVer);
          }
        } else {
          log.debug("Copy from src {} (v:{}) to dst {} (v:{}) is needed due to a higher version.",
              originFileStatus.getPath(), srcVer, copyToFileMap.get(newPath).getPath(), dstVer);
        }
      } else {
        log.debug("Copy from src {} to dst {} is needed because dst doesn't contain the file",
            originFileStatus.getPath(), copyToFileMap.get(newPath));
      }

      if (shouldCopy) {
        // need to remove those files in the target File System
        if (copyToFileMap.containsKey(newPath)) {
          deletedPaths.add(newPath);
        }
        CopyableFile copyableFile = CopyableFile
            .fromOriginAndDestination(copyFromFs, originFileStatus, copyToFs.makeQualified(newPath), copyConfiguration)
            .fileSet(PathUtils.getPathWithoutSchemeAndAuthority(copyTo.getDatasetPath()).toString())
            .dataFileVersionStrategy(this.versionStrategyFromCS.isPresent()? this.versionStrategyFromCS.get(): null)
            .build();
        copyableFile.setFsDatasets(copyFromFs, copyToFs);
        copyableFiles.add(copyableFile);
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
      copyableFiles.add(
          new PrePublishStep(copyTo.getDatasetPath().toString(), Maps.<String, String>newHashMap(), deleteCommitStep,
              0));
    }

    // generate the watermark file even if watermark checking is disabled. Make sure it can come into functional once disired.
    if ((!watermarkMetadataCopied) && copyFrom.getWatermark().isPresent()) {
      copyableFiles.add(new PostPublishStep(copyTo.getDatasetPath().toString(), Maps.<String, String>newHashMap(),
          new WatermarkMetadataGenerationCommitStep(copyTo.getFsURI().toString(), copyTo.getDatasetPath(),
              copyFrom.getWatermark().get()), 1));
    }
    return copyableFiles;
  }
}
