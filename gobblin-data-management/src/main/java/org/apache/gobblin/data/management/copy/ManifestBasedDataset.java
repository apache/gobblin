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

package org.apache.gobblin.data.management.copy;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.data.management.copy.entities.PrePublishStep;
import org.apache.gobblin.data.management.partition.FileSet;
import org.apache.gobblin.util.commit.DeleteFileCommitStep;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * A dataset that based on Manifest. We expect the Manifest contains the list of all the files for this dataset.
 * At first phase, we only support copy across different clusters to the same location. (We can add more feature to support rename in the future)
 * We will delete the file on target if it's listed in the manifest and not exist on source when {@link ManifestBasedDataset.DELETE_FILE_NOT_EXIST_ON_SOURCE} set to be true
 */
@Slf4j
public class ManifestBasedDataset implements IterableCopyableDataset {

  private static final String DELETE_FILE_NOT_EXIST_ON_SOURCE = ManifestBasedDatasetFinder.CONFIG_PREFIX + ".deleteFileNotExistOnSource";
  private static final String COMMON_FILES_PARENT = ManifestBasedDatasetFinder.CONFIG_PREFIX + ".commonFilesParent";
  private static final String PERMISSION_CACHE_TTL_SECONDS = ManifestBasedDatasetFinder.CONFIG_PREFIX + ".permission.cache.ttl.seconds";
  private static final String DEFAULT_PERMISSION_CACHE_TTL_SECONDS = "30";
  private static final String DEFAULT_COMMON_FILES_PARENT = "/";
  private final FileSystem fs;
  private final Path manifestPath;
  private final Properties properties;
  private final boolean deleteFileThatNotExistOnSource;
  private final String commonFilesParent;
  private final int permissionCacheTTLSeconds;

  public ManifestBasedDataset(final FileSystem fs, Path manifestPath, Properties properties) {
    this.fs = fs;
    this.manifestPath = manifestPath;
    this.properties = properties;
    this.deleteFileThatNotExistOnSource = Boolean.parseBoolean(properties.getProperty(DELETE_FILE_NOT_EXIST_ON_SOURCE, "false"));
    this.commonFilesParent = properties.getProperty(COMMON_FILES_PARENT, DEFAULT_COMMON_FILES_PARENT);
    this.permissionCacheTTLSeconds = Integer.parseInt(properties.getProperty(PERMISSION_CACHE_TTL_SECONDS, DEFAULT_PERMISSION_CACHE_TTL_SECONDS));
  }

  @Override
  public String datasetURN() {
    return this.manifestPath.toString();
  }

  @Override
  public Iterator<FileSet<CopyEntity>> getFileSetIterator(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {
    if (!fs.exists(manifestPath)) {
      throw new IOException(String.format("Manifest path %s does not exist on filesystem %s, skipping this manifest"
          + ", probably due to wrong configuration of %s", manifestPath.toString(), fs.getUri().toString(), ManifestBasedDatasetFinder.MANIFEST_LOCATION));
    } else if (fs.getFileStatus(manifestPath).isDirectory()) {
      throw new IOException(String.format("Manifest path %s on filesystem %s is a directory, which is not supported. Please set the manifest file locations in"
          + "%s, you can specify multi locations split by '',", manifestPath.toString(), fs.getUri().toString(), ManifestBasedDatasetFinder.MANIFEST_LOCATION));
    }
    CopyManifest.CopyableUnitIterator manifests = null;
    List<CopyEntity> copyEntities = Lists.newArrayList();
    List<FileStatus> toDelete = Lists.newArrayList();
    //todo: put permission preserve logic here?
    try {
      long startTime = System.currentTimeMillis();
      manifests = CopyManifest.getReadIterator(this.fs, this.manifestPath);
      Cache<String, OwnerAndPermission> permissionMap = CacheBuilder.newBuilder().expireAfterAccess(permissionCacheTTLSeconds, TimeUnit.SECONDS).build();
      int numFiles = 0;
      while (manifests.hasNext()) {
        numFiles++;
        CopyManifest.CopyableUnit file = manifests.next();
        //todo: We can use fileSet to partition the data in case of some softbound issue
        //todo: After partition, change this to directly return iterator so that we can save time if we meet resources limitation
        Path fileToCopy = new Path(file.fileName);
        if (fs.exists(fileToCopy)) {
          boolean existOnTarget = targetFs.exists(fileToCopy);
          FileStatus srcFile = fs.getFileStatus(fileToCopy);
          OwnerAndPermission replicatedPermission = CopyableFile.resolveReplicatedOwnerAndPermission(fs, srcFile, configuration);
          if (!existOnTarget || shouldCopy(targetFs, srcFile, targetFs.getFileStatus(fileToCopy), replicatedPermission)) {
            CopyableFile.Builder copyableFileBuilder =
                CopyableFile.fromOriginAndDestination(fs, srcFile, fileToCopy, configuration)
                    .fileSet(datasetURN())
                    .datasetOutputPath(fileToCopy.toString())
                    .ancestorsOwnerAndPermission(
                        CopyableFile.resolveReplicatedOwnerAndPermissionsRecursivelyWithCache(fs, fileToCopy.getParent(),
                            new Path(commonFilesParent), configuration, permissionMap))
                    .destinationOwnerAndPermission(replicatedPermission);
            CopyableFile copyableFile = copyableFileBuilder.build();
            copyableFile.setFsDatasets(fs, targetFs);
            copyEntities.add(copyableFile);
            if (existOnTarget && srcFile.isFile()) {
              // this is to match the existing publishing behavior where we won't rewrite the target when it's already existed
              // todo: Change the publish behavior to support overwrite destination file during rename, instead of relying on this delete step which is needed if we want to support task level publish
              toDelete.add(targetFs.getFileStatus(fileToCopy));
            }
          }
        } else if (deleteFileThatNotExistOnSource && targetFs.exists(fileToCopy)) {
          toDelete.add(targetFs.getFileStatus(fileToCopy));
        }
      }
      if (!toDelete.isEmpty()) {
        //todo: add support sync for empty dir
        CommitStep step = new DeleteFileCommitStep(targetFs, toDelete, this.properties, Optional.<Path>absent());
        copyEntities.add(new PrePublishStep(datasetURN(), Maps.newHashMap(), step, 1));
      }
      log.info(String.format("Workunits calculation took %s milliseconds to process %s files", System.currentTimeMillis() - startTime, numFiles));
    } catch (JsonIOException| JsonSyntaxException e) {
      //todo: update error message to point to a sample json file instead of schema which is hard to understand
      log.warn(String.format("Failed to read Manifest path %s on filesystem %s, please make sure it's in correct json format with schema"
          + " {type:array, items:{type: object, properties:{id:{type:String}, fileName:{type:String}, fileGroup:{type:String}, fileSizeInBytes: {type:Long}}}}",
          manifestPath.toString(), fs.getUri().toString()), e);
      throw new IOException(e);
    } catch (Exception e ) {
      log.warn(String.format("Failed to process Manifest path %s on filesystem %s, due to", manifestPath.toString(), fs.getUri().toString()), e);
      throw new IOException(e);
    } finally {
      if (manifests != null) {
        manifests.close();
      }
    }
    return Collections.singleton(new FileSet.Builder<>(datasetURN(), this).add(copyEntities).build()).iterator();
  }

  private static boolean shouldCopy(FileSystem targetFs, FileStatus fileInSource, FileStatus fileInTarget, OwnerAndPermission replicatedPermission)
      throws IOException {
    if (fileInSource.isDirectory() || fileInSource.getModificationTime() == fileInTarget.getModificationTime()) {
      // if source is dir or source and dst has same version, we compare the permission to determine whether it needs another sync
      return !replicatedPermission.hasSameOwnerAndPermission(targetFs, fileInTarget);
    }
    return fileInSource.getModificationTime() > fileInTarget.getModificationTime();
  }
}
