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

  public static final String DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_BLACKLIST = "%s.%s is mismatched. User input for %s: '%s' is in the blacklist. Please check the provided blacklist configuration.";

  /**
   * The populateErrorForDatasetDescriptorKey function will compare the submitted variables and add associated errors to the error array.
   * @param errors list of errors
   * @param inputDataset whether it's the input or output
   * @param configKey DatasetDescriptorConfigKeys key of the field fed into the function
   * @param inputDatasetDescriptorValue the property from the flow.conf
   * @param providedDatasetDescriptorValue the property from the submitted flow configuration
   * @param testNullOnly flag that is true if we only want to test if a property is null or not
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

  /**
   * The populateErrorForDatasetDescriptorKeyPartition function will compare the submitted variables and add associated errors to the error array.
   * @param errors list of errors
   * @param inputDataset whether it's the input or output
   * @param configKey DatasetDescriptorConfigKeys key of the field fed into the function
   * @param partitionConfigKey the subkey for the partition (e.g. partition.pattern)
   * @param inputDatasetDescriptorValue the property from the flow.conf
   * @param providedDatasetDescriptorValue the property from the submitted flow configuration
   * @param testNullOnly flag that is true if we only want to test if a property is null or not
   */
  public static void populateErrorForDatasetDescriptorKeyPartition(ArrayList<String> errors, Boolean inputDataset,
      String configKey, String partitionConfigKey, String inputDatasetDescriptorValue, String providedDatasetDescriptorValue, Boolean testNullOnly) {
    String datasetDescriptorPrefix = inputDataset ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    if (providedDatasetDescriptorValue == null) {
      errors.add(String.format(DATASET_DESCRIPTOR_KEY_MISSING_ERROR_TEMPLATE_PARTITION, datasetDescriptorPrefix, configKey, partitionConfigKey, inputDatasetDescriptorValue));
    }

    if (!testNullOnly && !(DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(inputDatasetDescriptorValue)
        || inputDatasetDescriptorValue.equalsIgnoreCase(providedDatasetDescriptorValue))) {
      errors.add(String.format(DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_PARTITION, datasetDescriptorPrefix, configKey, partitionConfigKey, providedDatasetDescriptorValue, inputDatasetDescriptorValue));
    }
  }

  /**
   * The populateErrorForDatasetDescriptorKeyRegex function will compare the submitted variables using the regex matching method and add associated errors to the error array.
   * @param errors list of errors
   * @param inputDataset whether it's the input or output
   * @param configKey DatasetDescriptorConfigKeys key of the field fed into the function
   * @param inputDatasetDescriptorValue the property from the flow.conf
   * @param providedDatasetDescriptorValue the property from the submitted flow configuration
   */
  public static void populateErrorForDatasetDescriptorKeyRegex(ArrayList<String> errors, Boolean inputDataset,
      String configKey, String inputDatasetDescriptorValue, String providedDatasetDescriptorValue) {
    String datasetDescriptorPrefix = inputDataset ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    if (!Pattern.compile(inputDatasetDescriptorValue).matcher(providedDatasetDescriptorValue).matches()) {
      errors.add(String.format(DatasetDescriptorErrorUtils.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE, datasetDescriptorPrefix, configKey, providedDatasetDescriptorValue, inputDatasetDescriptorValue));
    }
  }

  /**
   * The populateErrorForDatasetDescriptorKeyBlacklist function will check whether the database and/or table is in the blacklist config.
   * @param errors list of errors
   * @param inputDataset whether it's the input or output
   * @param type whether it's the database or the table within a database that the function is checking
   * @param configKey DatasetDescriptorConfigKeys key of the field fed into the function
   * @param whitelistBlacklist whitelistblacklist object for filtering hive based tables
   * @param inputDbName the database name from the submitted flow configuration
   * @param inputTableName the table name from the submitted flow configuration
   */
  public static void populateErrorForDatasetDescriptorKeyBlacklist(ArrayList<String> errors, Boolean inputDataset,
      String type, String configKey, WhitelistBlacklist whitelistBlacklist, String inputDbName, String inputTableName) {
    String datasetDescriptorPrefix = inputDataset ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    if (type.equals("database") && !whitelistBlacklist.acceptDb(inputDbName)) {
      errors.add(String.format(DatasetDescriptorErrorUtils.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_BLACKLIST,
          datasetDescriptorPrefix, "database", configKey, inputDbName));
    } else if (type.equals("table") && !whitelistBlacklist.acceptTable(inputDbName, inputTableName)) {
      errors.add(String.format(DatasetDescriptorErrorUtils.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_BLACKLIST,
          datasetDescriptorPrefix, "table", configKey, String.join(".", inputDbName, inputTableName)));
    }
  }

  /**
   *
   * @param errors list of errors
   * @param inputDataset whether it's the input or output
   * @param configKey DatasetDescriptorConfigKeys key of the field fed into the function
   * @param parts the list of parts after splitting using the separation character
   * @param inputPath the path from the submitted flow configuration
   * @param sepChar the delimiter/separation character
   * @param size the expected size of the list of parts
   */
  public static void populateErrorForDatasetDescriptorKeySize(ArrayList<String> errors, Boolean inputDataset,
      String configKey, List<String> parts, String inputPath, String sepChar, int size) {
    String datasetDescriptorPrefix = inputDataset ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    if (parts.size() != size) {
      errors.add(String.format(DatasetDescriptorErrorUtils.DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_STRING_SPLIT, datasetDescriptorPrefix, configKey, inputPath, sepChar, size));
    }
  }
}
