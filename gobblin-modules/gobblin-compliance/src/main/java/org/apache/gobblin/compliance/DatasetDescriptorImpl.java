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
package org.apache.gobblin.compliance;

import java.util.List;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import lombok.extern.slf4j.Slf4j;


/**
 * This is the default implementation of {@link DatasetDescriptor}
 *
 * Descriptor is the JsonString corresponding to the value of dataset.descriptor.
 * ComplianceFieldPath is the path to the complianceField in the dataset.descriptor.
 * ComplianceFieldPath must not contain array element as it is not supported.
 *
 * @author adsharma
 */
@Slf4j
public class DatasetDescriptorImpl extends DatasetDescriptor {
  private static final Splitter DOT_SPLITTER = Splitter.on(".").omitEmptyStrings().trimResults();
  private String complianceField;

  public DatasetDescriptorImpl(String descriptor, Optional<String> complianceFieldPath) {
    super(descriptor, complianceFieldPath);
    setComplianceField();
  }

  private void setComplianceField() {
    Preconditions.checkArgument(this.complianceFieldPath.isPresent());
    try {
      JsonObject descriptorObject = new JsonParser().parse(this.descriptor).getAsJsonObject();
      List<String> list = DOT_SPLITTER.splitToList(this.complianceFieldPath.get());
      for (int i = 0; i < list.size() - 1; i++) {
        descriptorObject = descriptorObject.getAsJsonObject(list.get(i));
      }
      this.complianceField = descriptorObject.get(list.get(list.size() - 1)).getAsString();
    } catch (JsonParseException | NullPointerException e) {
      log.warn("Compliance field not found at path " + this.complianceFieldPath.get() + " in the descriptor "
          + this.descriptor);
      Throwables.propagate(e);
    }
  }

  @Override
  public String getComplianceField() {
    return this.complianceField;
  }
}
