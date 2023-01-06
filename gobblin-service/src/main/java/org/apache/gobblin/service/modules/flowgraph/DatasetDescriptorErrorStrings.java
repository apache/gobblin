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
public class DatasetDescriptorErrorStrings {
  public static final String DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE = "%s.%s is mismatched. User input: %s. Expected value '%s'.";
  public static final String DATASET_DESCRIPTOR_KEY_MISSING_ERROR_TEMPLATE = "%s.%s is missing. Expected value '%s'.";

  public static final String DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_PARTITION = "%s.%s.%s is mismatched. User input: %s. Expected value '%s'.";
  public static final String DATASET_DESCRIPTOR_KEY_MISSING_ERROR_TEMPLATE_PARTITION = "%s.%s.%s is missing. Expected value '%s'.";

  public static final String DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_STRING_SPLIT = "%s.%s is mismatched. User input: %s is not splittable. Expected separation character: '%s' and total of %d parts.";

  public static final String DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_IS_GLOB_PATTERN = "%s.%s is mismatched. User input: %s is of a glob pattern. Expected input is not of a glob pattern.";
  public static final String DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_GLOB_PATTERN = "%s.%s is mismatched. User input: %s is not contained within the glob of %s.";

  public static final String DATASET_DESCRIPTOR_KEY_MISMATCH_ERROR_TEMPLATE_BLACKLIST = "%s.%s is mismatched. User input for %s: '%s' is in the blacklist";
}
