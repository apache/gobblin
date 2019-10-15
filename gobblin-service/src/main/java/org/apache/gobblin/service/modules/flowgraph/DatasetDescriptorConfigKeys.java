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

package org.apache.gobblin.service.modules.flowgraph;

/**
 * Config keys related to {@link org.apache.gobblin.service.modules.dataset.DatasetDescriptor}.
 */
public class DatasetDescriptorConfigKeys {
  //Gobblin Service Dataset Descriptor related keys
  public static final String FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX = "gobblin.flow.input.dataset.descriptor";
  public static final String FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX = "gobblin.flow.output.dataset.descriptor";

  public static final String FLOW_EDGE_INPUT_DATASET_DESCRIPTOR_PREFIX = "gobblin.flow.edge.input.dataset.descriptor";
  public static final String FLOW_EDGE_OUTPUT_DATASET_DESCRIPTOR_PREFIX = "gobblin.flow.edge.output.dataset.descriptor";

  public static final String CLASS_KEY = "class";
  public static final String PLATFORM_KEY = "platform";
  public static final String PATH_KEY = "path";
  public static final String SUBPATHS_KEY = "subPaths";
  public static final String DATABASE_KEY = "databaseName";
  public static final String TABLE_KEY = "tableName";
  public static final String FORMAT_KEY = "format";
  public static final String CODEC_KEY = "codec";
  public static final String DESCRIPTION_KEY = "description";
  public static final String IS_RETENTION_APPLIED_KEY = "isRetentionApplied";
  public static final String IS_COMPACTED_KEY = "isCompacted";
  public static final String IS_COMPACTED_AND_DEDUPED_KEY = "isCompactedAndDeduped";

  //Dataset encryption related keys
  public static final String ENCYPTION_PREFIX = "encrypt";
  public static final String ENCRYPTION_ALGORITHM_KEY = "algorithm";
  public static final String ENCRYPTION_KEYSTORE_TYPE_KEY = "keystore_type";
  public static final String ENCRYPTION_KEYSTORE_ENCODING_KEY = "keystore_encoding";
  public static final String ENCRYPTION_LEVEL_KEY = "level";
  public static final String ENCRYPTED_FIELDS = "encryptedFields";

  //Dataset partition related keys
  public static final String PARTITION_PREFIX = "partition";
  public static final String PARTITION_TYPE_KEY = "type";
  public static final String PARTITION_PATTERN_KEY = "pattern";

  public static final String DATASET_DESCRIPTOR_CONFIG_ANY = "any";
  public static final String DATASET_DESCRIPTOR_CONFIG_NONE = "none";
}
