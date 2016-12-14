/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
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
  private JsonObject descriptor;
  private String identifierField;
  private final Splitter DOT_SPLITTER = Splitter.on(".").omitEmptyStrings().trimResults();

  public DatasetDescriptorImpl(String descriptor, String identifierField) {
    this.descriptor = new JsonParser().parse(descriptor).getAsJsonObject();
    this.identifierField = identifierField;
  }

  /**
   * This returns the column name corresponding to the compliance id specified in the dataset descriptor.
   * If descriptor is {"a" : {"b" : {"c" : "d"}}}, and identifierField is a.b.c then d is returned.
   *
   * Array is not supported.
   * @return String
   */
  public String getComplianceId() {
    JsonObject object = this.descriptor;
    List<String> list = DOT_SPLITTER.splitToList(this.identifierField);
    for (int i = 0; i < list.size() - 1; i++) {
      object = object.getAsJsonObject(list.get(i));
    }
    return object.get(list.get(list.size() - 1)).getAsString();
  }
}
