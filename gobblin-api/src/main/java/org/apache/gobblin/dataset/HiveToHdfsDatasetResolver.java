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

package org.apache.gobblin.dataset;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.gobblin.configuration.State;


/**
 * Singleton {@link DatasetResolver} to convert a Hive {@link DatasetDescriptor} to HDFS {@link DatasetDescriptor}
 */
public class HiveToHdfsDatasetResolver implements DatasetResolver {
  public static final String HIVE_TABLE = "hiveTable";
  public static final HiveToHdfsDatasetResolver INSTANCE = new HiveToHdfsDatasetResolver();

  private HiveToHdfsDatasetResolver() {
    // To make it singleton
  }

  @Override
  public DatasetDescriptor resolve(DatasetDescriptor raw, State state) {
    ImmutableMap<String, String> metadata = raw.getMetadata();
    Preconditions.checkArgument(metadata.containsKey(DatasetConstants.FS_SCHEME),
        String.format("Hive Dataset Descriptor must contain metadata %s to create Hdfs Dataset Descriptor",
            DatasetConstants.FS_SCHEME));
    Preconditions.checkArgument(metadata.containsKey(DatasetConstants.FS_SCHEME),
        String.format("Hive Dataset Descriptor must contain metadata %s to create Hdfs Dataset Descriptor",
            DatasetConstants.FS_LOCATION));
    DatasetDescriptor datasetDescriptor =
        new DatasetDescriptor(metadata.get(DatasetConstants.FS_SCHEME), metadata.get(DatasetConstants.FS_LOCATION));
    datasetDescriptor.addMetadata(HIVE_TABLE, raw.getName());
    return datasetDescriptor;
  }
}
