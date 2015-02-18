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

package gobblin.test;

import java.io.IOException;
import java.util.Collection;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.publisher.DataPublisher;


/**
 * An implementation of {@link DataPublisher} for integration test.
 *
 * <p>
 *     This is a dummy implementation that exists purely to make
 *     integration test work.
 * </p>
 */
public class TestDataPublisher extends DataPublisher {

  public TestDataPublisher(State state) {
    super(state);
  }

  @Override
  public void initialize()
      throws IOException {
    // Do nothing
  }

  @Override
  public void close()
      throws IOException {
    // Do nothing
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> tasks)
      throws IOException {
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> tasks)
      throws IOException {
  }
}