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

package org.apache.gobblin.service.modules.dataset;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.ArrayList;
import lombok.Getter;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorErrorUtils;


/**
 * Describes a external dataset not on HDFS, for usage with Data-Integration-Library in a generic way - see: https://github.com/linkedin/data-integration-library/tree/master
 * Datasets under ExternalUriDatasetDescriptor can also be represented by more specific dataset descriptors, e.g. HttpDatasetDescriptor, SqlDatasetDescriptor, etc.
 * e.g, https://some-api:443/user/123/names for a http URI
 * e.g, jdbc:mysql://some-db:3306/db for a sql URI
 */
public class ExternalUriDatasetDescriptor extends BaseDatasetDescriptor implements DatasetDescriptor {

  @Getter
  private final String uri;

  public ExternalUriDatasetDescriptor(Config config) throws IOException {
    super(config);
    Preconditions.checkArgument(config.hasPath(DatasetDescriptorConfigKeys.URI_KEY), "Dataset descriptor config must specify a uri");
    // refers to an external URI of a given dataset, see https://github.com/linkedin/data-integration-library/blob/master/docs/parameters/ms.source.uri.md
    this.uri = config.getString(DatasetDescriptorConfigKeys.URI_KEY);
  }

  @Override
  public String getPath() {
    return this.uri;
  }

  /**
   * Check if this dataset descriptor is equivalent to another dataset descriptor
   *
   * @param inputDatasetDescriptorConfig whose path should be in the format of an external path (e.g. http url)
   */
  @Override
  protected ArrayList<String> isPathContaining(DatasetDescriptor inputDatasetDescriptorConfig) {
    ArrayList<String> errors = new ArrayList<>();
    String otherPath = inputDatasetDescriptorConfig.getPath();
    DatasetDescriptorErrorUtils.populateErrorForDatasetDescriptorKey(errors, inputDatasetDescriptorConfig.getIsInputDataset(), DatasetDescriptorConfigKeys.PATH_KEY, this.getPath(), otherPath, false);
    return errors;
  }
}
