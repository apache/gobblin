/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor;

import java.io.IOException;

import gobblin.configuration.WorkUnitState;
import gobblin.instrumented.extractor.InstrumentedExtractor;


/**
 * Dummy extractor that always returns 0 records.
 */
public class DummyExtractor<S, D> extends InstrumentedExtractor<S, D> {

  public DummyExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
  }

  @Override
  public S getSchema() throws IOException {
    return null;
  }

  @Override
  public long getExpectedRecordCount() {
    return 0;
  }

  @Override
  public long getHighWatermark() {
    return 0;
  }

  @Override
  public D readRecordImpl(D reuse) throws DataRecordException, IOException {
    return null;
  }
}
