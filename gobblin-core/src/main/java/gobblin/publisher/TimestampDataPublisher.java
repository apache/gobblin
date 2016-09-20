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

package gobblin.publisher;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;


/**
 * Write output to {finaldir}/{currentTimestamp}
 */
public class TimestampDataPublisher extends BaseDataPublisher {

  private final String timestamp;

  public TimestampDataPublisher(State state) throws IOException {
    super(state);
    timestamp = String.valueOf(System.currentTimeMillis());
  }

  @Override
  protected Path getPublisherOutputDir(WorkUnitState workUnitState, int branchId) {
    Path outputDir = super.getPublisherOutputDir(workUnitState, branchId);
    outputDir = new Path(outputDir, timestamp);
    return outputDir;
  }
}
