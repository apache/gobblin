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

package gobblin.metadata.provider;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.typesafe.config.Config;

import gobblin.broker.iface.ConfigView;
import gobblin.broker.iface.ScopeType;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.configuration.ConfigurationKeys;
import gobblin.util.ClassAliasResolver;
import gobblin.util.ConfigUtils;
import gobblin.util.filesystem.FileSystemInstrumentation;
import gobblin.util.filesystem.FileSystemInstrumentationFactory;
import gobblin.util.filesystem.FileSystemKey;

import lombok.extern.slf4j.Slf4j;


/**
 * Extends {@link FileSystemInstrumentation} and is metadata aware when setting permissions and owners.
 */
@Slf4j
public class MetadataAwareFileSystem extends FileSystemInstrumentation {

  public static final String METADATA_PROVIDER_ALIAS = "metadataProviderAlias";

  public static class Factory<S extends ScopeType<S>> extends FileSystemInstrumentationFactory<S> {
    @Override
    public FileSystem instrumentFileSystem(FileSystem fs, SharedResourcesBroker<S> broker,
        ConfigView<S, FileSystemKey> config) {
      Config metaConfig = config.getConfig();
      String metadataProviderAlias = ConfigUtils.getString(metaConfig, METADATA_PROVIDER_ALIAS, "");
      log.info("Metadata provider alias is: " + metadataProviderAlias);
      if (!metadataProviderAlias.isEmpty()) {
        DatasetAwareFsMetadataProvider metadataProvider = null;
        try {
          metadataProvider =
              (DatasetAwareFsMetadataProvider) new ClassAliasResolver<>(DatasetAwareMetadataProviderFactory.class)
                  .resolveClass(metadataProviderAlias).newInstance().createMetadataProvider(metaConfig);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
          log.error("Failed to create metadataProvider.", e);
        }
        if (metadataProvider != null) {
          return new MetadataAwareFileSystem(fs, metadataProvider);
        }
      }
      log.warn("No valid {} found. Will use filesystem {}.", METADATA_PROVIDER_ALIAS, fs.getClass().getName());
      return fs;
    }
  }

  private DatasetAwareFsMetadataProvider metadataProvider;

  public MetadataAwareFileSystem(FileSystem underlying, DatasetAwareFsMetadataProvider provider) {
    super(underlying);
    this.metadataProvider = provider;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission)
      throws IOException {
    FsPermission realPermission = getPermAtPathFromMetadataIfPresent(permission, f);
    return super.mkdirs(f, realPermission);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress)
      throws IOException {
    FsPermission realPermission = getPermAtPathFromMetadataIfPresent(permission, f);
    return super.create(f, realPermission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public void setPermission(Path f, final FsPermission permission)
      throws IOException {
    super.setPermission(f, getPermAtPathFromMetadataIfPresent(permission, f));
  }

  @Override
  public void setOwner(Path f, String user, String group)
      throws IOException {
    super.setOwner(f, user, getGroupAtPathFromMetadataIfPresent(group, f));
  }

  private String getGroupAtPathFromMetadataIfPresent(String defaultGroup, Path path) {
    String realGroup = defaultGroup;
    if (this.metadataProvider != null) {
      realGroup = PermissionMetadataParser.getGroupOwner(metadataProvider.getGlobalMetadataForDatasetAtPath(path));
    }
    return realGroup;
  }

  private FsPermission getPermAtPathFromMetadataIfPresent(FsPermission defaultPermission, Path path) {
    FsPermission realPermission = defaultPermission;
    if (this.metadataProvider != null) {
      realPermission = new FsPermission(Short
          .parseShort(PermissionMetadataParser.getPermission(metadataProvider.getGlobalMetadataForDatasetAtPath(path)),
              ConfigurationKeys.PERMISSION_PARSING_RADIX));
    }
    return realPermission;
  }
}
