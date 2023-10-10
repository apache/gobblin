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
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.stream.JsonWriter;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import org.apache.gobblin.data.management.copy.PreserveAttributes.Option;
import org.apache.gobblin.data.management.partition.File;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.dataset.Descriptor;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.guid.Guid;


/**
 * Abstraction for a file to copy from {@link #origin} to {@link #destination}. {@link CopyableFile}s should be
 * created using a {@link CopyableFile.Builder} obtained with the method {@link CopyableFile#builder}.
 */
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class CopyableFile extends CopyEntity implements File {
  private static final byte[] EMPTY_CHECKSUM = new byte[0];

  /**
   * The source data the file belongs to. For now, since it's only used before copying, set it to be
   * transient so that it won't be serialized, avoid unnecessary data transfer
   */
  private transient Descriptor sourceData;

  /** {@link FileStatus} of the existing origin file. */
  private FileStatus origin;

  /** The destination data the file will be copied to */
  private Descriptor destinationData;

  /** Complete destination {@link Path} of the file. */
  private Path destination;

  /** Common path for dataset to which this CopyableFile belongs. */
  public String datasetOutputPath;

  /** Desired {@link OwnerAndPermission} of the destination path. */
  private OwnerAndPermission destinationOwnerAndPermission;

  /**
   * Desired {@link OwnerAndPermission} of the ancestor directories of the destination path. The list is ordered from
   * deepest to highest directory.
   *
   * <p>
   * For example, if {@link #destination} is /a/b/c/file, then the first element of this list is the desired owner and
   * permission for directory /a/b/c, the second is the desired owner and permission for directory /a/b, and so on.
   * </p>
   *
   * <p>
   * If there are fewer elements in the list than ancestor directories in {@link #destination}, it is understood that
   * extra directories are allowed to have any owner and permission.
   * </p>
   */
  private List<OwnerAndPermission> ancestorsOwnerAndPermission;

  /** Checksum of the origin file. */
  private byte[] checksum;
  /** Attributes to be preserved. */
  private PreserveAttributes preserve;
  /** Timestamp of file at its origin source. */
  private long originTimestamp;
  /** Timestamp of file as in upstream. */
  private long upstreamTimestamp;

  private String dataFileVersionStrategy;

  @lombok.Builder(builderClassName = "Builder", builderMethodName = "_hiddenBuilder")
  public CopyableFile(FileStatus origin, Path destination, OwnerAndPermission destinationOwnerAndPermission,
      List<OwnerAndPermission> ancestorsOwnerAndPermission, byte[] checksum, PreserveAttributes preserve,
      String fileSet, long originTimestamp, long upstreamTimestamp, Map<String, String> additionalMetadata,
      String datasetOutputPath,
      String dataFileVersionStrategy) {
    super(fileSet, additionalMetadata);
    this.origin = origin;
    this.destination = destination;
    this.destinationOwnerAndPermission = destinationOwnerAndPermission;
    this.ancestorsOwnerAndPermission = ancestorsOwnerAndPermission;
    this.checksum = checksum;
    this.preserve = preserve;
    this.dataFileVersionStrategy = dataFileVersionStrategy;
    this.originTimestamp = originTimestamp;
    this.upstreamTimestamp = upstreamTimestamp;
    this.datasetOutputPath = datasetOutputPath;
  }

  /** @return Stringified form, including metadata, in pretty-printed JSON */
  public String toJsonString() {
    return toJsonString(true);
  }

  public String toJsonString(boolean includeMetadata) {
    StringWriter stringWriter = new StringWriter();
    try (JsonWriter jsonWriter = new JsonWriter(stringWriter)) {
      jsonWriter.setIndent("\t");
      this.toJson(jsonWriter, includeMetadata);
    } catch (IOException ioe) {
      // Ignored
    }
    return stringWriter.toString();
  }

  public void toJson(JsonWriter jsonWriter, boolean includeMetadata) throws IOException {
    jsonWriter.beginObject();

    jsonWriter
        .name("file set").value(this.getFileSet())
        .name("origin").value(this.getOrigin().toString())
        .name("destination").value(this.getDestination().toString())
        .name("destinationOwnerAndPermission").value(this.getDestinationOwnerAndPermission().toString())
        // TODO:
        // this.ancestorsOwnerAndPermission
        // this.checksum
        // this.preserve
        // this.dataFileVersionStrategy
        // this.originTimestamp
        // this.upstreamTimestamp
        .name("datasetOutputPath").value(this.getDatasetOutputPath().toString());

    if (includeMetadata && this.getAdditionalMetadata() != null) {
      jsonWriter.name("metadata");
      jsonWriter.beginObject();
      for (Map.Entry<String, String> entry : this.getAdditionalMetadata().entrySet()) {
        jsonWriter.name(entry.getKey()).value(entry.getValue());
      }
      jsonWriter.endObject();
    }

    jsonWriter.endObject();
  }

  /**
   * Set file system based source and destination dataset for this {@link CopyableFile}
   *
   * @param originFs {@link FileSystem} where this {@link CopyableFile} origins
   * @param targetFs {@link FileSystem} where this {@link CopyableFile} is copied to
   */
  public void setFsDatasets(FileSystem originFs, FileSystem targetFs) {
    /*
     * By default, the raw Gobblin dataset for CopyableFile lineage is its parent folder
     * if itself is not a folder
     */
    boolean isDir = origin.isDirectory();

    Path fullSourcePath = Path.getPathWithoutSchemeAndAuthority(origin.getPath());
    String sourceDatasetName = isDir ? fullSourcePath.toString() : fullSourcePath.getParent().toString();
    DatasetDescriptor sourceDataset = new DatasetDescriptor(originFs.getScheme(), originFs.getUri(), sourceDatasetName);
    sourceDataset.addMetadata(DatasetConstants.FS_URI, originFs.getUri().toString());
    sourceData = sourceDataset;

    Path fullDestinationPath = Path.getPathWithoutSchemeAndAuthority(destination);
    String destinationDatasetName = isDir ? fullDestinationPath.toString() : fullDestinationPath.getParent().toString();
    DatasetDescriptor destinationDataset = new DatasetDescriptor(targetFs.getScheme(), targetFs.getUri(),
            destinationDatasetName);
    destinationDataset.addMetadata(DatasetConstants.FS_URI, targetFs.getUri().toString());
    destinationData = destinationDataset;
  }

  /**
   * Get a {@link CopyableFile.Builder}.
   *
   * @param originFs {@link FileSystem} where original file exists.
   * @param origin {@link FileStatus} of the original file.
   * @param datasetRoot Value of {@link CopyableDataset#datasetRoot} of the dataset creating this {@link CopyableFile}.
   * @param copyConfiguration {@link CopyConfiguration} for the copy job.
   * @return a {@link CopyableFile.Builder}.
   * @deprecated use {@link #fromOriginAndDestination}. This method was changed to remove reliance on dataset root
   *             which is not standard of all datasets. The old functionality on inferring destinations cannot be
   *             achieved without dataset root and common dataset root, so this is an approximation. Copyable datasets
   *             should compute file destinations themselves.
   */
  @Deprecated
  public static Builder builder(FileSystem originFs, FileStatus origin, Path datasetRoot,
      CopyConfiguration copyConfiguration) {

    Path relativePath = PathUtils.relativizePath(origin.getPath(), datasetRoot);

    Path targetRoot = new Path(copyConfiguration.getPublishDir(), datasetRoot.getName());
    Path targetPath = new Path(targetRoot, relativePath);

    return _hiddenBuilder().originFS(originFs).origin(origin).destination(targetPath)
        .preserve(copyConfiguration.getPreserve()).configuration(copyConfiguration);
  }

  public static Builder fromOriginAndDestination(FileSystem originFs, FileStatus origin, Path destination,
      CopyConfiguration copyConfiguration) {
    return _hiddenBuilder().originFS(originFs).origin(origin).destination(destination).configuration(copyConfiguration)
        .preserve(copyConfiguration.getPreserve());
  }

  /**
   * Builder for creating {@link CopyableFile}s.
   *
   * Allows the {@link CopyableDataset} to set any field of the {@link CopyableFile}, but infers any unset fields
   * to facilitate creation of custom {@link CopyableDataset}s. See javadoc for {@link CopyableFile.Builder#build} for
   * inference information.
   */
  public static class Builder {

    private CopyConfiguration configuration;
    private FileSystem originFs;
    private Map<String, String> additionalMetadata;
    private String datasetOutputPath;

    private Builder originFS(FileSystem originFs) {
      this.originFs = originFs;
      return this;
    }

    private Builder configuration(CopyConfiguration configuration) {
      this.configuration = configuration;
      return this;
    }

    /**
     * Builds a {@link CopyableFile} using fields set by the {@link CopyableDataset} and inferring unset fields.
     * If the {@link Builder} was obtained through {@link CopyableFile#builder}, it is safe to call this method
     * even without setting any other fields (they will all be inferred).
     *
     * <p>
     *   The inferred fields are as follows:
     *   * {@link CopyableFile#destinationOwnerAndPermission}: Copy attributes from origin {@link FileStatus} depending
     *       on the {@link PreserveAttributes} flags {@link #preserve}. Non-preserved attributes are left null,
     *       allowing Gobblin distcp to use defaults for the target {@link FileSystem}.
     *   * {@link CopyableFile#ancestorsOwnerAndPermission}: Copy attributes from ancestors of origin path whose name
     *       exactly matches the corresponding name in the target path and which don't exist on th target. The actual
     *       owner and permission depend on the {@link PreserveAttributes} flags {@link #preserve}.
     *       Non-preserved attributes are left null, allowing Gobblin distcp to use defaults for the target {@link FileSystem}.
     *   * {@link CopyableFile#checksum}: the checksum of the origin {@link FileStatus} obtained using the origin
     *       {@link FileSystem}.
     *   * {@link CopyableFile#fileSet}: empty string. Used as default file set per dataset.
     * </p>
     *
     * @return A {@link CopyableFile}.
     * @throws IOException
     */
    public CopyableFile build() throws IOException {

      if (!this.destination.isAbsolute()) {
        throw new IOException("Destination must be absolute: " + this.destination);
      }

      if (this.destinationOwnerAndPermission == null) {
        String owner = this.preserve.preserve(Option.OWNER) ? this.origin.getOwner() : null;

        String group = null;
        if (this.preserve.preserve(Option.GROUP)) {
          group = this.origin.getGroup();
        } else if (this.configuration.getTargetGroup().isPresent()) {
          group = this.configuration.getTargetGroup().get();
        }

        FsPermission permission = this.preserve.preserve(Option.PERMISSION) ? this.origin.getPermission() : null;
        List<AclEntry> aclEntries = this.preserve.preserve(Option.ACLS) ? getAclEntries(this.originFs, this.origin.getPath()) : Lists.newArrayList();

        this.destinationOwnerAndPermission = new OwnerAndPermission(owner, group, permission, aclEntries);
      }
      if (this.ancestorsOwnerAndPermission == null) {
        this.ancestorsOwnerAndPermission = replicateAncestorsOwnerAndPermission(this.originFs, this.origin.getPath(),
            this.configuration.getTargetFs(), this.destination);
      }
      if (this.checksum == null) {
        if (ConfigUtils.getBoolean(this.configuration.getConfig(), "copy.skipChecksum", true)) {
          this.checksum = EMPTY_CHECKSUM;
        } else {
          FileChecksum checksumTmp = this.origin.isDirectory() ? null : this.originFs.getFileChecksum(this.origin.getPath());
          this.checksum = checksumTmp == null ? EMPTY_CHECKSUM : checksumTmp.getBytes();
        }
      }
      if (this.fileSet == null) {
        // Default file set per dataset
        this.fileSet = "";
      }
      if (this.originTimestamp == 0) {
        this.originTimestamp = this.origin.getModificationTime();
      }
      if (this.upstreamTimestamp == 0) {
        this.upstreamTimestamp = this.origin.getModificationTime();
      }

      return new CopyableFile(this.origin, this.destination, this.destinationOwnerAndPermission,
          this.ancestorsOwnerAndPermission, this.checksum, this.preserve, this.fileSet, this.originTimestamp,
          this.upstreamTimestamp, this.additionalMetadata, this.datasetOutputPath, this.dataFileVersionStrategy);
    }

    private List<OwnerAndPermission> replicateAncestorsOwnerAndPermission(FileSystem originFs, Path originPath,
        FileSystem targetFs, Path destinationPath) throws IOException {

      List<OwnerAndPermission> ancestorOwnerAndPermissions = Lists.newArrayList();

      Path currentOriginPath = originPath.getParent();
      Path currentTargetPath = destinationPath.getParent();

      while (currentOriginPath != null && currentTargetPath != null
          && currentOriginPath.getName().equals(currentTargetPath.getName())) {

        Optional<FileStatus> targetFileStatus =
            this.configuration.getCopyContext().getFileStatus(targetFs, currentTargetPath);

        if (targetFileStatus.isPresent()) {
          return ancestorOwnerAndPermissions;
        }

        ancestorOwnerAndPermissions
            .add(resolveReplicatedOwnerAndPermission(originFs, currentOriginPath, this.configuration));

        currentOriginPath = currentOriginPath.getParent();
        currentTargetPath = currentTargetPath.getParent();
      }

      return ancestorOwnerAndPermissions;
    }

  }

  /**
   * Computes the correct {@link OwnerAndPermission} obtained from replicating source owner and permissions and applying
   * the {@link PreserveAttributes} rules in copyConfiguration.
   * @throws IOException
   */
  public static OwnerAndPermission resolveReplicatedOwnerAndPermission(FileSystem fs, Path path,
      CopyConfiguration copyConfiguration) throws IOException {
    Optional<FileStatus> originFileStatus = copyConfiguration.getCopyContext().getFileStatus(fs, path);

    if (!originFileStatus.isPresent()) {
      throw new IOException(String.format("Origin path %s does not exist.", path));
    }

    return resolveReplicatedOwnerAndPermission(fs, originFileStatus.get(), copyConfiguration);
  }

  /**
   * Computes the correct {@link OwnerAndPermission} obtained from replicating source owner and permissions and applying
   * the {@link PreserveAttributes} rules in copyConfiguration.
   * @throws IOException
   */
  public static OwnerAndPermission resolveReplicatedOwnerAndPermission(FileSystem fs, FileStatus originFileStatus,
      CopyConfiguration copyConfiguration)
      throws IOException {

    PreserveAttributes preserve = copyConfiguration.getPreserve();
    String group = null;
    if (copyConfiguration.getTargetGroup().isPresent()) {
      group = copyConfiguration.getTargetGroup().get();
    } else if (preserve.preserve(Option.GROUP)) {
      group = originFileStatus.getGroup();
    }

    return new OwnerAndPermission(preserve.preserve(Option.OWNER) ? originFileStatus.getOwner() : null, group,
        preserve.preserve(Option.PERMISSION) ? originFileStatus.getPermission() : null,
        preserve.preserve(Option.ACLS) ? getAclEntries(fs, originFileStatus.getPath()) : Lists.newArrayList());
  }

  /**
   * Compute the correct {@link OwnerAndPermission} obtained from replicating source owner and permissions and applying
   * the {@link PreserveAttributes} rules for fromPath and every ancestor up to but excluding toPath.
   *
   * @return A list of the computed {@link OwnerAndPermission}s starting from fromPath, up to but excluding toPath.
   * @throws IOException if toPath is not an ancestor of fromPath.
   */
  public static List<OwnerAndPermission> resolveReplicatedOwnerAndPermissionsRecursively(FileSystem sourceFs, Path fromPath,
      Path toPath, CopyConfiguration copyConfiguration) throws IOException {

    if (!PathUtils.isAncestor(toPath, fromPath)) {
      throw new IOException(String.format("toPath %s must be an ancestor of fromPath %s.", toPath, fromPath));
    }

    List<OwnerAndPermission> ownerAndPermissions = Lists.newArrayList();
    Path currentPath = fromPath;

    while (currentPath.getParent() != null && PathUtils.isAncestor(toPath, currentPath.getParent())) {
      ownerAndPermissions.add(resolveReplicatedOwnerAndPermission(sourceFs, currentPath, copyConfiguration));
      currentPath = currentPath.getParent();
    }

    return ownerAndPermissions;
  }

  /**
   * Compute the correct {@link OwnerAndPermission} obtained from replicating source owner and permissions and applying
   * the {@link PreserveAttributes} rules for fromPath and every ancestor up to but excluding toPath.
   * Unlike the resolveReplicatedOwnerAndPermissionsRecursively() method, this method utilizes permissionMap as a cache to minimize the number of calls to HDFS.
   * It is recommended to use this method when recursively calculating permissions for numerous files that share the same ancestor.
   *
   * @return A list of the computed {@link OwnerAndPermission}s starting from fromPath, up to but excluding toPath.
   * @throws IOException if toPath is not an ancestor of fromPath.
   */
  public static List<OwnerAndPermission> resolveReplicatedOwnerAndPermissionsRecursivelyWithCache(FileSystem sourceFs, Path fromPath,
      Path toPath, CopyConfiguration copyConfiguration, Cache<String, OwnerAndPermission> permissionMap)
      throws IOException, ExecutionException {

    if (!PathUtils.isAncestor(toPath, fromPath)) {
      throw new IOException(String.format("toPath %s must be an ancestor of fromPath %s.", toPath, fromPath));
    }

    List<OwnerAndPermission> ownerAndPermissions = Lists.newArrayList();
    Path currentPath = fromPath;

    while (currentPath.getParent() != null && PathUtils.isAncestor(toPath, currentPath.getParent())) {
      Path finalCurrentPath = currentPath;
      ownerAndPermissions.add(permissionMap.get(finalCurrentPath.toString(), () -> resolveReplicatedOwnerAndPermission(sourceFs,
          finalCurrentPath, copyConfiguration)));
      currentPath = currentPath.getParent();
    }

    return ownerAndPermissions;
  }

  public static Map<String, OwnerAndPermission> resolveReplicatedAncestorOwnerAndPermissionsRecursively(FileSystem sourceFs, Path fromPath,
      Path toPath, CopyConfiguration copyConfiguration) throws IOException {
    Preconditions.checkArgument(sourceFs.getFileStatus(fromPath).isDirectory(), "Source path must be a directory.");

    Map<String, OwnerAndPermission> ownerAndPermissions = Maps.newHashMap();

    // We only pass directories to this method anyways. Those directories themselves need permissions set.
    Path currentOriginPath = fromPath;
    Path currentTargetPath = toPath;

    if (!PathUtils.isAncestor(currentTargetPath, currentOriginPath)) {
      throw new IOException(String.format("currentTargetPath %s must be an ancestor of currentOriginPath %s.", currentTargetPath, currentOriginPath));
    }

    while (PathUtils.isAncestor(currentTargetPath, currentOriginPath.getParent())) {
      ownerAndPermissions.put(PathUtils.getPathWithoutSchemeAndAuthority(currentOriginPath).toString(), resolveReplicatedOwnerAndPermission(sourceFs, currentOriginPath, copyConfiguration));
      currentOriginPath = currentOriginPath.getParent();
    }

    // Walk through the parents and preserve the permissions from Origin -> Target as we go in lockstep.
    while (currentOriginPath != null && currentTargetPath != null
        && currentOriginPath.getName().equals(currentTargetPath.getName())) {
      ownerAndPermissions.put(PathUtils.getPathWithoutSchemeAndAuthority(currentOriginPath).toString(), resolveReplicatedOwnerAndPermission(sourceFs, currentOriginPath, copyConfiguration));
      currentOriginPath = currentOriginPath.getParent();
      currentTargetPath = currentTargetPath.getParent();
    }

    return ownerAndPermissions;
  }

  private static List<AclEntry> getAclEntries(FileSystem srcFs, Path path) throws IOException {
    AclStatus aclStatus = srcFs.getAclStatus(path);
    return aclStatus.getEntries();
  }

  @Override
  public FileStatus getFileStatus() {
    return this.origin;
  }

  /**
   * @return desired block size for destination file.
   */
  public long getBlockSize(FileSystem targetFs) {
    return getPreserve().preserve(PreserveAttributes.Option.BLOCK_SIZE) ?
        getOrigin().getBlockSize() : targetFs.getDefaultBlockSize(this.destination);
  }

  /**
   * @return desired replication for destination file.
   */
  public short getReplication(FileSystem targetFs) {
    return getPreserve().preserve(PreserveAttributes.Option.REPLICATION) ?
        getOrigin().getReplication() : targetFs.getDefaultReplication(this.destination);
  }

  /**
   * Generates a replicable guid to uniquely identify the origin of this {@link CopyableFile}.
   * @return a guid uniquely identifying the origin file.
   */
  @Override
  public Guid guid() throws IOException {
    StringBuilder uniqueString = new StringBuilder();
    uniqueString.append(getFileStatus().getModificationTime());
    uniqueString.append(getFileStatus().getLen());
    uniqueString.append(getFileStatus().getPath());
    return Guid.fromStrings(uniqueString.toString());
  }

  @Override
  public String explain() {
    String owner = this.destinationOwnerAndPermission != null && this.destinationOwnerAndPermission.getOwner() != null
        ? this.destinationOwnerAndPermission.getOwner() : "preserve";
    String group = this.destinationOwnerAndPermission != null && this.destinationOwnerAndPermission.getGroup() != null
        ? this.destinationOwnerAndPermission.getGroup() : "preserve";
    String permissions =
        this.destinationOwnerAndPermission != null && this.destinationOwnerAndPermission.getFsPermission() != null
            ? this.destinationOwnerAndPermission.getFsPermission().toString() : "preserve";
    return String.format("Copy file %s to %s with owner %s, group %s, permission %s.", this.origin.getPath(),
        this.destination, owner, group, permissions);
  }
}
