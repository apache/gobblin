/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.publisher;

import java.io.IOException;
import java.util.Collection;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;


/**
 * An extension of {@link BaseDataPublisher} which performs Hive registration after publishing data.
 *
 * <p>
 *   This publisher should generally be used as the job level data publisher, since doing Hive registration
 *   in tasks may need to create many Hive metastore connections if the number of tasks is large. To publish
 *   data in tasks and do Hive registration in the driver, one should use
 *   {@link BaseDataPublisher} as the task level publisher and
 *   {@link HiveRegistrationPublisher} as the job level publisher.
 * </p>
 *
 * @author Ziyang Liu
 */
public class BaseDataPublisherWithHiveRegistration extends BaseDataPublisher {

  protected final HiveRegistrationPublisher hivePublisher;

  public BaseDataPublisherWithHiveRegistration(State state) throws IOException {
    super(state);
    this.hivePublisher = this.closer.register(new HiveRegistrationPublisher(state));
  }

  @Override
  public void publish(Collection<? extends WorkUnitState> states) throws IOException {
    super.publish(states);
    this.hivePublisher.publish(states);
  }

}
