/* (c) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.kafka;

import java.io.IOException;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.extract.EventBasedExtractor;


public abstract class KafkaExtractor<S, D> extends EventBasedExtractor<S, D> {

  public KafkaExtractor(WorkUnitState state) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public S getSchema() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public D readRecord(D reuse) throws DataRecordException, IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getExpectedRecordCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getHighWatermark() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

}
