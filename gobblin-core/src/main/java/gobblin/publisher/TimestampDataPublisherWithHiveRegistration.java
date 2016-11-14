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
import java.util.Collection;
import org.apache.hadoop.fs.Path;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;


/**
 * Wrapper for {@link TimestampDataPublisher} to publish with {@link HiveRegistrationPublisher} afterwards.
 */
public class TimestampDataPublisherWithHiveRegistration extends TimestampDataPublisher {

  private final HiveRegistrationPublisher hivePublisher;

  public TimestampDataPublisherWithHiveRegistration(State state) throws IOException {
    super(state);
    this.hivePublisher = this.closer.register(new HiveRegistrationPublisher(state));
  }

  @Override
  public void publish(Collection<? extends WorkUnitState> states) throws IOException {
    super.publish(states);

    // PUBLISHER_DIRS key must be updated for HiveRegistrationPublisher
    for (Path path : this.publisherOutputDirs) {
      this.state.appendToSetProp(ConfigurationKeys.PUBLISHER_DIRS, path.toString());
    }

    this.hivePublisher.publish(states);
  }
}
