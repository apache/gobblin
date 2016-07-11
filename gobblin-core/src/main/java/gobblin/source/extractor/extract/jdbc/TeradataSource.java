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

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.QueryBasedSource;
import gobblin.source.extractor.exception.ExtractPrepareException;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

/**
 * An implementation of Teradata source to get work units
 *
 * @author ypopov
 */
@Slf4j
public class TeradataSource extends QueryBasedSource<JsonArray, JsonElement> {

  public Extractor<JsonArray, JsonElement> getExtractor(WorkUnitState state)
      throws IOException {
    Extractor<JsonArray, JsonElement> extractor = null;
    try {
      extractor = new TeradataExtractor(state).build();
    } catch (ExtractPrepareException e) {
      log.error("Failed to prepare extractor: error - {}", e.getMessage());
      throw new IOException(e);
    }
    return extractor;
  }
}