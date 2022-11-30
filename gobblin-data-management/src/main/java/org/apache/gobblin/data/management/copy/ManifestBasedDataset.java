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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
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
  private final FileSystem fs;
  private final Path manifestPath;
  private final Properties properties;
  private final boolean deleteFileThatNotExistOnSource;
  private Gson GSON = new Gson();

  public ManifestBasedDataset(final FileSystem fs, Path manifestPath, Properties properties) {
    this.fs = fs;
    this.manifestPath = manifestPath;
    this.properties = properties;
    this.deleteFileThatNotExistOnSource = Boolean.parseBoolean(properties.getProperty(DELETE_FILE_NOT_EXIST_ON_SOURCE, "false"));
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
    JsonReader reader = null;
    List<CopyEntity> copyEntities = Lists.newArrayList();
    List<FileStatus> toDelete = Lists.newArrayList();
    //todo: put permission preserve logic here?
    try {
      reader = new JsonReader(new InputStreamReader(fs.open(manifestPath), "UTF-8"));
      reader.beginArray();
      while (reader.hasNext()) {
        //todo: We can use fileSet to partition the data in case of some softbound issue
        //todo: After partition, change this to directly return iterator so that we can save time if we meet resources limitation
        JsonObject file = GSON.fromJson(reader, JsonObject.class);
        Path fileToCopy = new Path(file.get("fileName").getAsString());
        if (this.fs.exists(fileToCopy)) {
          if (!targetFs.exists(fileToCopy) || shouldCopy(this.fs.getFileStatus(fileToCopy), targetFs.getFileStatus(fileToCopy))) {
            CopyableFile copyableFile =
                CopyableFile.fromOriginAndDestination(this.fs, this.fs.getFileStatus(fileToCopy), fileToCopy, configuration)
                    .fileSet(datasetURN())
                    .datasetOutputPath(fileToCopy.toString())
                    .ancestorsOwnerAndPermission(CopyableFile
                        .resolveReplicatedOwnerAndPermissionsRecursively(this.fs, fileToCopy.getParent(),
                            new Path("/"), configuration))
                    .build();
            copyableFile.setFsDatasets(this.fs, targetFs);
            copyEntities.add(copyableFile);
          }
        } else if (this.deleteFileThatNotExistOnSource && targetFs.exists(fileToCopy)){
          toDelete.add(targetFs.getFileStatus(fileToCopy));
        }
      }
      if (this.deleteFileThatNotExistOnSource) {
        //todo: add support sync for empty dir
        CommitStep step = new DeleteFileCommitStep(targetFs, toDelete, this.properties, Optional.<Path>absent());
        copyEntities.add(new PrePublishStep(datasetURN(), Maps.newHashMap(), step, 1));
      }
    } catch (JsonIOException| JsonSyntaxException e) {
      //todo: update error message to point to a sample json file instead of schema which is hard to understand
      log.warn(String.format("Failed to read Manifest path %s on filesystem %s, please make sure it's in correct json format with schema"
          + " {type:array, items:{type: object, properties:{id:{type:String}, fileName:{type:String}, fileGroup:{type:String}, fileSizeInBytes: {type:Long}}}}",
          manifestPath.toString(), fs.getUri().toString()), e);
      throw new IOException(e);
    } catch (Exception e ) {
      log.warn(String.format("Failed to process Manifest path %s on filesystem %s, due to", manifestPath.toString(), fs.getUri().toString()), e);
      throw new IOException(e);
    }finally {
      if(reader != null) {
        reader.endArray();
        reader.close();
      }
    }
    return Collections.singleton(new FileSet.Builder<>(datasetURN(), this).add(copyEntities).build()).iterator();
  }

  private static boolean shouldCopy(FileStatus fileInSource, FileStatus fileInTarget) {
    //todo: make the rule configurable
    return fileInSource.getModificationTime() > fileInTarget
        .getModificationTime();
  }
}
