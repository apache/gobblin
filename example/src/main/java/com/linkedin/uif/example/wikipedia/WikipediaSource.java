/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.uif.example.wikipedia;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.google.gson.JsonElement;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.Source;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.workunit.Extract;
import com.linkedin.uif.source.workunit.WorkUnit;
import com.linkedin.uif.source.workunit.Extract.TableType;

/**
 * An implementation of {@link Source} for the Wikipedia example.
 *
 * <p>
 *   This source creates a {@link com.linkedin.uif.source.workunit.WorkUnit}, and uses 
 *   {@link WikipediaExtractor} to pull the data from Wikipedia.
 * </p>
 *
 * @author ziliu
 */
public class WikipediaSource implements Source<String, JsonElement> {

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    Extract extract = state
            .createExtract(TableType.SNAPSHOT_ONLY, state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY),
                "WikipediaOutput");

    WorkUnit workUnit = new WorkUnit(state, extract);
    return Arrays.asList(workUnit);
  }

  @Override
  public Extractor<String, JsonElement> getExtractor(WorkUnitState state)
      throws IOException {
    return new WikipediaExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {
    //nothing to do
  }
}
