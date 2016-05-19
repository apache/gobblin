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
package gobblin.data.management.convertion.hive;

import java.io.IOException;
import java.util.Collection;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.configuration.WorkUnitState.WorkingState;
import gobblin.publisher.DataPublisher;
import gobblin.source.extractor.extract.LongWatermark;


/**
 * A simple {@link DataPublisher} updates the watermark and working state
 */
public class HiveConvertPublisher extends DataPublisher {
  public HiveConvertPublisher(State state) {
    super(state);
  }

  @Override
  public void initialize() throws IOException {}

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
    for (WorkUnitState wus : states) {
      wus.setWorkingState(WorkingState.COMMITTED);
      wus.setActualHighWatermark(
          TableLevelWatermarker.GSON.fromJson(wus.getWorkunit().getExpectedHighWatermark(), LongWatermark.class));
    }
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {}

  @Override
  public void close() throws IOException {}
}
