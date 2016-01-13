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

package gobblin.policies.avro;

import gobblin.configuration.State;
import gobblin.policies.time.RecordTimestampLowerBoundPolicy;
import gobblin.writer.partitioner.TimeBasedAvroWriterPartitioner;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;

/**
 * An implementation of {@link RecordTimestampLowerBoundPolicy} for Avro records.
 *
 * @author ziliu
 */
public class AvroRecordTimestampLowerBoundPolicy extends RecordTimestampLowerBoundPolicy {

  public AvroRecordTimestampLowerBoundPolicy(State state, Type type) {
    super(state, type);
  }

  @Override
  protected TimeBasedWriterPartitioner<?> getPartitioner() {
    return new TimeBasedAvroWriterPartitioner(this.state);
  }

}
