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

import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


public class DatasetDescriptorUtils {
  /**
   * Given dataset descriptor config, construct a {@link DatasetDescriptor} object
   */
  public static DatasetDescriptor constructDatasetDescriptor(Config descriptorConfig) throws ReflectiveOperationException {
    Class datasetDescriptorClass = Class.forName(descriptorConfig.getString(DatasetDescriptorConfigKeys.CLASS_KEY));
    return (DatasetDescriptor) GobblinConstructorUtils.invokeLongestConstructor(datasetDescriptorClass, descriptorConfig);
  }
}
