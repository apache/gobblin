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

import org.apache.hadoop.fs.Path;

import gobblin.annotation.Alpha;
import gobblin.metadata.types.GlobalMetadata;

import lombok.RequiredArgsConstructor;


/**
 * Simple implementation of {@link DatasetAwareMetadataProvider}, which directly
 * reads global permission from config.
 */
@Alpha
@RequiredArgsConstructor
public class SimpleConfigMetadataProvider extends DatasetAwareFsMetadataProvider {

  private final String permission;

  @Override
  public GlobalMetadata getGlobalMetadataForDataset(String datasetUrn) {
    GlobalMetadata defaultMetadata = new GlobalMetadata();
    defaultMetadata.setDatasetUrn(datasetUrn);
    PermissionMetadataParser.setPermission(defaultMetadata, permission);
    return defaultMetadata;
  }

  @Override
  public String datasetUrnAtPath(Path path) {
    return path.toString();
  }
}
