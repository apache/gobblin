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

import com.typesafe.config.Config;
import java.io.IOException;
import java.util.ArrayList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorErrorUtils;
import org.apache.gobblin.util.ConfigUtils;

/**
 * An implementation of {@link FSVolumeDatasetDescriptor} with fs.uri specified.
 */
@Alpha
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class FSVolumeDatasetDescriptor extends FSDatasetDescriptor{
  @Getter
  private final String fsUri;

  public FSVolumeDatasetDescriptor(Config config) throws IOException {
    super(config);
    this.fsUri = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.FS_URI_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY);
    this.isInputDataset = ConfigUtils.getBoolean(config, DatasetDescriptorConfigKeys.IS_INPUT_DATASET, false);
  }

  @Override
  public ArrayList<String> contains(DatasetDescriptor inputDatasetDescriptorConfig) {
    ArrayList<String> errors = new ArrayList<>();
    if (super.contains(inputDatasetDescriptorConfig).size() != 0) {
      return super.contains(inputDatasetDescriptorConfig);
    }

    FSVolumeDatasetDescriptor other = (FSVolumeDatasetDescriptor) inputDatasetDescriptorConfig;
    DatasetDescriptorErrorUtils.populateErrorForDatasetDescriptorKey(errors, inputDatasetDescriptorConfig.getIsInputDataset(), DatasetDescriptorConfigKeys.FS_URI_KEY, this.getFsUri(), other.getFsUri(), false);
    return errors;
  }

}
