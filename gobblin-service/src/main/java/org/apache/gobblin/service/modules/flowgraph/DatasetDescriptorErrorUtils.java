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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.gobblin.data.management.copy.hive.WhitelistBlacklist;


/**
 * Config keys related to {@link org.apache.gobblin.service.modules.dataset.DatasetDescriptor}.
 */
public class DatasetDescriptorErrorUtils {
  public static final String DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE = "%s.%s is mismatched. User input: %s. Expected value '%s'.";
  public static final String DATASET_DESCRIPTOR_KEY_MISSING_ERROR_TEMPLATE = "%s.%s is missing. Expected value '%s'.";

  public static final String DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_PARTITION = "%s.%s.%s is mismatched. User input: %s. Expected value '%s'.";
  public static final String DATASET_DESCRIPTOR_KEY_MISSING_ERROR_TEMPLATE_PARTITION = "%s.%s.%s is missing. Expected value '%s'.";

  public static final String DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_STRING_SPLIT = "%s.%s is mismatched. User input: %s is not splittable. Expected separation character: '%s' and total of %d parts.";

  public static final String DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_IS_GLOB_PATTERN = "%s.%s is mismatched. User input: %s is of a glob pattern. Expected input is not of a glob pattern.";
  public static final String DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_GLOB_PATTERN = "%s.%s is mismatched. User input: %s is not contained within the glob of %s.";

  public static final String DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_BLACKLIST = "%s.%s is mismatched. User input for %s: '%s' is in the blacklist";

  /**
   * The populateErrorForDatasetDescriptorKey function will compare the submitted variables and add associated errors to the error array called from .contains
   * @param errors list of errors
   * @param inputDataset whether it's the input or output
   * @param configKey DatasetDescriptorConfigKeys key of the field fed into the fucntion
   * @param inputDatasetDescriptorValue the property from the flow.conf
   * @param providedDatasetDescriptorValue the property from the submitted flow configuration
   */
  public static void populateErrorForDatasetDescriptorKey(ArrayList<String> errors, Boolean inputDataset,
      String configKey, String inputDatasetDescriptorValue, String providedDatasetDescriptorValue, Boolean testNullOnly) {
    String datasetDescriptorPrefix = inputDataset ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    if (providedDatasetDescriptorValue == null) {
      errors.add(String.format(DATASET_DESCRIPTOR_KEY_MISSING_ERROR_TEMPLATE, datasetDescriptorPrefix, configKey, inputDatasetDescriptorValue));
    }

    if (!testNullOnly && !(DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(inputDatasetDescriptorValue)
        || inputDatasetDescriptorValue.equalsIgnoreCase(providedDatasetDescriptorValue))) {
      errors.add(String.format(DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE, datasetDescriptorPrefix, configKey, providedDatasetDescriptorValue, inputDatasetDescriptorValue));
    }
  }

  public static void populateErrorForDatasetDescriptorKeyPartition(ArrayList<String> errors, Boolean inputDataset,
      String configKey, String partitionConfigKey, String inputDatasetDescriptorValue, String providedDatasetDescriptorValue, Boolean testNullOnly) {
    String datasetDescriptorPrefix = inputDataset ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    if (providedDatasetDescriptorValue == null) {
      errors.add(String.format(DATASET_DESCRIPTOR_KEY_MISSING_ERROR_TEMPLATE_PARTITION, datasetDescriptorPrefix, configKey, partitionConfigKey, inputDatasetDescriptorValue));
      return;
    }
    if (!(DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(inputDatasetDescriptorValue)
        || inputDatasetDescriptorValue.equalsIgnoreCase(providedDatasetDescriptorValue))) {
      errors.add(String.format(DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_PARTITION, datasetDescriptorPrefix, configKey, partitionConfigKey, providedDatasetDescriptorValue, inputDatasetDescriptorValue));
    }
  }

  public static void populateErrorForDatasetDescriptorKeyBlacklist(ArrayList<String> errors, Boolean inputDataset,
      String type, String configKey, String inputDatasetDescriptorValue, String providedDatasetDescriptorValue, String platform) {
    String datasetDescriptorPrefix = inputDataset ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    if (!platform.equals("sql")) {
      if (!inputDatasetDescriptorValue.equals(providedDatasetDescriptorValue)) {
        errors.add(String.format(DatasetDescriptorErrorUtils.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_BLACKLIST,
            datasetDescriptorPrefix, type, configKey, providedDatasetDescriptorValue));
      }
    }
    else {
      if (!Pattern.compile(inputDatasetDescriptorValue).matcher(providedDatasetDescriptorValue).matches()) {
        errors.add(String.format(DatasetDescriptorErrorUtils.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_BLACKLIST,
            datasetDescriptorPrefix, type, configKey, providedDatasetDescriptorValue));
      }
    }
  }

  public static void populateErrorForDatasetDescriptorKeyBlacklist(ArrayList<String> errors, Boolean inputDataset,
      String type, String configKey, WhitelistBlacklist whitelistBlacklist, String otherDbName, String otherTableName) {
    String datasetDescriptorPrefix = inputDataset ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    if (type.equals("database")) {
      if (!whitelistBlacklist.acceptDb(otherDbName)) {
        errors.add(String.format(DatasetDescriptorErrorUtils.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_BLACKLIST,
            datasetDescriptorPrefix, "database", configKey, otherDbName));
      }
    } else if (type.equals("table")) {
      if (!whitelistBlacklist.acceptTable(otherDbName, otherTableName)) {
        errors.add(String.format(DatasetDescriptorErrorUtils.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_BLACKLIST,
            datasetDescriptorPrefix, "table", configKey, String.join(".", otherDbName, otherTableName)));
      }
    }
  }

  public static void populateErrorForDatasetDescriptorKeySize(ArrayList<String> errors, Boolean inputDataset,
      List<String> parts, String otherPath, String sepChar, int size) {
    String datasetDescriptorPrefix = inputDataset ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    if (parts.size() != size) {
      errors.add(String.format(DatasetDescriptorErrorUtils.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_STRING_SPLIT, datasetDescriptorPrefix, DatasetDescriptorConfigKeys.PATH_KEY, otherPath, sepChar, size));
    }
  }
}
