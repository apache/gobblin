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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.data.management.copy.entities.PostPublishStep;
import org.apache.gobblin.data.management.copy.entities.PrePublishStep;
import org.apache.gobblin.data.management.partition.FileSet;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.commit.CreateDirectoryWithPermissionsCommitStep;
import org.apache.gobblin.util.commit.DeleteFileCommitStep;
import org.apache.gobblin.util.commit.SetPermissionCommitStep;
import org.apache.gobblin.util.filesystem.OwnerAndPermission;


/**
 * A dataset that based on Manifest. We expect the Manifest contains the list of all the files for this dataset.
 * At first phase, we only support copy across different clusters to the same location. (We can add more feature to support rename in the future)
 * We will delete the file on target if it's listed in the manifest and not exist on source when {@link ManifestBasedDataset#DELETE_FILE_NOT_EXIST_ON_SOURCE} set to be true
 */
@Slf4j
public class ManifestBasedDataset implements IterableCopyableDataset {

  private static final String DELETE_FILE_NOT_EXIST_ON_SOURCE = ManifestBasedDatasetFinder.CONFIG_PREFIX + ".deleteFileNotExistOnSource";
  private static final String COMMON_FILES_PARENT = ManifestBasedDatasetFinder.CONFIG_PREFIX + ".commonFilesParent";
  private static final String PERMISSION_CACHE_TTL_SECONDS = ManifestBasedDatasetFinder.CONFIG_PREFIX + ".permission.cache.ttl.seconds";

  // Enable setting permission post publish to reset permission bits, default is true
  private static final String ENABLE_SET_PERMISSION_POST_PUBLISH = ManifestBasedDatasetFinder.CONFIG_PREFIX + ".enableSetPermissionPostPublish";
  public static final String SKIP_PERMISSION_CHECK = ManifestBasedDatasetFinder.CONFIG_PREFIX + ".skipPermissionCheck";
  private static final String DEFAULT_PERMISSION_CACHE_TTL_SECONDS = "30";
  private static final String DEFAULT_COMMON_FILES_PARENT = "/";
  private static final boolean DEFAULT_SKIP_PERMISSION_CHECK = false;
  private final FileSystem srcFs;
  private final FileSystem manifestReadFs;
  private final Path manifestPath;
  private final Properties properties;
  private final boolean deleteFileThatNotExistOnSource;
  private final String commonFilesParent;
  private final int permissionCacheTTLSeconds;

  private final boolean enableSetPermissionPostPublish;
  private final boolean skipPermissionCheck;

  public ManifestBasedDataset(final FileSystem srcFs, final FileSystem manifestReadFs, final Path manifestPath, final Properties properties) {
    this.srcFs = srcFs;
    this.manifestReadFs = manifestReadFs;
    this.manifestPath = manifestPath;
    this.properties = properties;
    this.deleteFileThatNotExistOnSource = Boolean.parseBoolean(properties.getProperty(DELETE_FILE_NOT_EXIST_ON_SOURCE, "false"));
    this.commonFilesParent = properties.getProperty(COMMON_FILES_PARENT, DEFAULT_COMMON_FILES_PARENT);
    this.permissionCacheTTLSeconds = Integer.parseInt(properties.getProperty(PERMISSION_CACHE_TTL_SECONDS, DEFAULT_PERMISSION_CACHE_TTL_SECONDS));
    this.enableSetPermissionPostPublish = Boolean.parseBoolean(properties.getProperty(ENABLE_SET_PERMISSION_POST_PUBLISH, "true"));
    this.skipPermissionCheck = Boolean.parseBoolean(properties.getProperty(SKIP_PERMISSION_CHECK, String.valueOf(DEFAULT_SKIP_PERMISSION_CHECK)));
  }

  @Override
  public String datasetURN() {
    return this.manifestPath.toString();
  }

  @Override
  public Iterator<FileSet<CopyEntity>> getFileSetIterator(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {
    if (!manifestReadFs.exists(manifestPath)) {
      throw new IOException(String.format("Manifest path %s does not exist on filesystem %s, skipping this manifest"
          + ", probably due to wrong configuration of %s or %s", manifestPath.toString(), manifestReadFs.getUri().toString(),
          ManifestBasedDatasetFinder.MANIFEST_LOCATION, ManifestBasedDatasetFinder.MANIFEST_READ_FS_URI));
    } else if (manifestReadFs.getFileStatus(manifestPath).isDirectory()) {
      throw new IOException(String.format("Manifest path %s on filesystem %s is a directory, which is not supported. Please set the manifest file locations in"
          + "%s, you can specify multi locations split by '',", manifestPath.toString(), manifestReadFs.getUri().toString(),
          ManifestBasedDatasetFinder.MANIFEST_LOCATION));
    }

    CopyManifest.CopyableUnitIterator manifests = null;
    List<CopyEntity> copyEntities = Lists.newArrayList();
    List<FileStatus> toDelete = Lists.newArrayList();
    // map of paths and permissions sorted by depth of path, so that permissions can be set in order
    Map<String, List<OwnerAndPermission>> ancestorOwnerAndPermissions = new HashMap<>();
    Map<String, List<OwnerAndPermission>> ancestorOwnerAndPermissionsForSetPermissionStep = new HashMap<>();
    Map<String, OwnerAndPermission> existingDirectoryPermissionsForSetPermissionStep = new HashMap<>();
    TreeMap<String, OwnerAndPermission> flattenedAncestorPermissions = new TreeMap<>(
        Comparator.comparingInt((String o) -> o.split("/").length).thenComparing(o -> o));
    try {
      long startTime = System.currentTimeMillis();
      manifests = CopyManifest.getReadIterator(this.manifestReadFs, this.manifestPath);
      Cache<String, OwnerAndPermission> permissionMap = CacheBuilder.newBuilder().expireAfterAccess(permissionCacheTTLSeconds, TimeUnit.SECONDS).build();
      int numFiles = 0;
      while (manifests.hasNext()) {
        numFiles++;
        CopyManifest.CopyableUnit file = manifests.next();
        //todo: We can use fileSet to partition the data in case of some softbound issue
        //todo: After partition, change this to directly return iterator so that we can save time if we meet resources limitation
        Path fileToCopy = new Path(file.fileName);
        if (srcFs.exists(fileToCopy)) {
          boolean existOnTarget = targetFs.exists(fileToCopy);
          if (this.skipPermissionCheck && existOnTarget) {
            // Skip Permission Check for files that already exist in the target when skipPermissionCheck is true
            continue;
          }
          FileStatus srcFile = srcFs.getFileStatus(fileToCopy);
          OwnerAndPermission replicatedPermission = CopyableFile.resolveReplicatedOwnerAndPermission(srcFs, srcFile, configuration);
          if (!existOnTarget || shouldCopy(targetFs, srcFile, targetFs.getFileStatus(fileToCopy), replicatedPermission)) {
            CopyableFile.Builder copyableFileBuilder =
                CopyableFile.fromOriginAndDestination(srcFs, srcFile, fileToCopy, configuration)
                    .fileSet(datasetURN())
                    .datasetOutputPath(fileToCopy.toString())
                    .ancestorsOwnerAndPermission(
                        CopyableFile.resolveReplicatedOwnerAndPermissionsRecursivelyWithCache(srcFs, fileToCopy.getParent(),
                            new Path(commonFilesParent), configuration, permissionMap))
                    .destinationOwnerAndPermission(replicatedPermission);
            CopyableFile copyableFile = copyableFileBuilder.build();
            copyableFile.setFsDatasets(srcFs, targetFs);
            copyEntities.add(copyableFile);

            // In case of directory with 000 permission, the permission is changed to 100 due to HadoopUtils::addExecutePermissionToOwner
            // getting called from CopyDataPublisher::preserveFileAttrInPublisher -> FileAwareInputStreamDataWriter::setPathPermission ->
            // FileAwareInputStreamDataWriter::setOwnerExecuteBitIfDirectory -> HadoopUtils::addExecutePermissionToOwner
            // We need to revert this extra permission change in setPermissionStep
            if (srcFile.isDirectory() && !srcFile.getPermission().getUserAction().implies(FsAction.EXECUTE)
                && !ancestorOwnerAndPermissionsForSetPermissionStep.containsKey(PathUtils.getPathWithoutSchemeAndAuthority(fileToCopy).toString())) {
              OwnerAndPermission srcFileOwnerPermissionReplicatedForDest = new OwnerAndPermission(copyableFile.getDestinationOwnerAndPermission());
              if(!targetFs.exists(fileToCopy)) {
                List<OwnerAndPermission> ancestorsOwnerAndPermissionUpdated = new ArrayList<>();
                ancestorsOwnerAndPermissionUpdated.add(srcFileOwnerPermissionReplicatedForDest);
                copyableFile.getAncestorsOwnerAndPermission().forEach(ancestorOwnPerm -> ancestorsOwnerAndPermissionUpdated.add(new OwnerAndPermission(ancestorOwnPerm)));
                ancestorOwnerAndPermissionsForSetPermissionStep.put(fileToCopy.toString(), ancestorsOwnerAndPermissionUpdated);
              }
              else {
                // If the path exists, update only current directory permission in post publish step and not entire hierarchy.
                existingDirectoryPermissionsForSetPermissionStep.put(fileToCopy.toString(), srcFileOwnerPermissionReplicatedForDest);
              }
            }

            // Always grab the parent since the above permission setting should be setting the permission for a folder itself
            // {@link CopyDataPublisher#preserveFileAttrInPublisher} will be setting the permission for the empty child dir
            Path fromPath = fileToCopy.getParent();
            // Avoid duplicate calculation for the same ancestor
            if (fromPath != null && !ancestorOwnerAndPermissions.containsKey(PathUtils.getPathWithoutSchemeAndAuthority(fromPath).toString()) && !targetFs.exists(fromPath)) {
              ancestorOwnerAndPermissions.put(fromPath.toString(), copyableFile.getAncestorsOwnerAndPermission());
              if (!ancestorOwnerAndPermissionsForSetPermissionStep.containsKey(PathUtils.getPathWithoutSchemeAndAuthority(fromPath).toString())) {
                ancestorOwnerAndPermissionsForSetPermissionStep.put(fromPath.toString(), copyableFile.getAncestorsOwnerAndPermission());
              }
            }

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

      // Precreate the directories to avoid an edge case where recursive rename can create extra directories in the target
      CommitStep createDirectoryWithPermissionsCommitStep = new CreateDirectoryWithPermissionsCommitStep(targetFs, ancestorOwnerAndPermissions, this.properties);
      copyEntities.add(new PrePublishStep(datasetURN(), Maps.newHashMap(), createDirectoryWithPermissionsCommitStep, 1));

      if (this.enableSetPermissionPostPublish) {
        for (Map.Entry<String, List<OwnerAndPermission>> recursiveParentPermissions : ancestorOwnerAndPermissionsForSetPermissionStep.entrySet()) {
          Path currentPath = new Path(recursiveParentPermissions.getKey());
          for (OwnerAndPermission ownerAndPermission : recursiveParentPermissions.getValue()) {
            // Ignore folders that already exist in destination, we assume that the publisher will re-sync those permissions if needed and
            // those folders should be added in the manifest.
            if (!flattenedAncestorPermissions.containsKey(currentPath.toString()) && !targetFs.exists(currentPath)) {
              flattenedAncestorPermissions.put(currentPath.toString(), ownerAndPermission);
            }
            currentPath = currentPath.getParent();
          }
        }
        for (Map.Entry<String, OwnerAndPermission> existingDirectoryPermissions : existingDirectoryPermissionsForSetPermissionStep.entrySet()) {
          Path currentPath = new Path(existingDirectoryPermissions.getKey());
          flattenedAncestorPermissions.put(currentPath.toString(), existingDirectoryPermissions.getValue());
        }
        CommitStep setPermissionCommitStep = new SetPermissionCommitStep(targetFs, flattenedAncestorPermissions, this.properties);
        copyEntities.add(new PostPublishStep(datasetURN(), Maps.newHashMap(), setPermissionCommitStep, 1));
      }
      if (!toDelete.isEmpty()) {
        //todo: add support sync for empty dir
        CommitStep step = new DeleteFileCommitStep(targetFs, toDelete, this.properties, Optional.<Path>absent());
        copyEntities.add(new PrePublishStep(datasetURN(), Maps.newHashMap(), step, 1));
      }
      log.info(String.format("Workunits calculation took %s milliseconds to process %s files", System.currentTimeMillis() - startTime, numFiles));
    } catch (JsonIOException | JsonSyntaxException e) {
      //todo: update error message to point to a sample json file instead of schema which is hard to understand
      log.warn(String.format("Failed to read Manifest path %s on filesystem %s, please make sure it's in correct json format with schema"
          + " {type:array, items:{type: object, properties:{id:{type:String}, fileName:{type:String}, fileGroup:{type:String}, fileSizeInBytes: {type:Long}}}}",
          manifestPath.toString(), manifestReadFs.getUri().toString()), e);
      throw new IOException(e);
    } catch (Exception e) {
      log.warn(String.format("Failed to process Manifest path %s on filesystem %s, due to", manifestPath.toString(), manifestReadFs.getUri().toString()), e);
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
    // Copy only if source is newer than target or if the owner or permission is different
    return fileInSource.getModificationTime() > fileInTarget.getModificationTime() || !replicatedPermission.hasSameOwnerAndPermission(targetFs, fileInTarget);
  }
}
