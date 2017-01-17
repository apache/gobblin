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
package gobblin.compliance;

import java.util.List;

import com.google.common.base.Splitter;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


/**
 * This is the default implementation of {@link DatasetDescriptor}
 *
 * @author adsharma
 */
public class DatasetDescriptorImpl implements DatasetDescriptor {
  private static final Splitter DOT_SPLITTER = Splitter.on(".").omitEmptyStrings().trimResults();
  private String complianceId;

  /**
   *
   * @param descriptor is the JsonString corresponding to the value of dataset.descriptor.
   * @param identifierField is the path to the identifier in the dataset.descriptor.
   */
  public DatasetDescriptorImpl(String descriptor, String identifierField) {
    JsonObject descriptorObject = new JsonParser().parse(descriptor).getAsJsonObject();
    List<String> list = DOT_SPLITTER.splitToList(identifierField);
    for (int i = 0; i < list.size() - 1; i++) {
      descriptorObject = descriptorObject.getAsJsonObject(list.get(i));
    }
    this.complianceId = descriptorObject.get(list.get(list.size() - 1)).getAsString();
  }

  /**
   * Array is not supported
   */
  public String getComplianceId() {
    return this.complianceId;
  }
}
