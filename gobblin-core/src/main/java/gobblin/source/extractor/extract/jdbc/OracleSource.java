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

package gobblin.source.extractor.extract.jdbc;

import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.exception.ExtractPrepareException;

import java.io.IOException;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.QueryBasedSource;

public class OracleSource extends QueryBasedSource<JsonArray, JsonElement> {

  @Override
  public Extractor<JsonArray, JsonElement> getExtractor(WorkUnitState state)
      throws IOException {
    try {
      return new OracleExtractor(state).build();
    } catch (ExtractPrepareException e) {
      throw new RuntimeException("Failed to prepare extractor", e);
    }
  }
}
