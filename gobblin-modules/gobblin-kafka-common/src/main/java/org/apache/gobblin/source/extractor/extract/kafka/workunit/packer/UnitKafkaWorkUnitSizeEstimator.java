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
package org.apache.gobblin.source.extractor.extract.kafka.workunit.packer;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.source.workunit.WorkUnit;

/**
 * A dummy implementation of {@link KafkaWorkUnitSizeEstimator} that directly returns unit("1") for
 * {@link #calcEstimatedSize(WorkUnit)} methods as the estimated size for each {@link WorkUnit}
 * could be useless in certain circumstances.
 */
public class UnitKafkaWorkUnitSizeEstimator implements KafkaWorkUnitSizeEstimator {
  public UnitKafkaWorkUnitSizeEstimator(SourceState state) {
    // do nothing
  }

  @Override
  public double calcEstimatedSize(WorkUnit workUnit) {
    return 1;
  }
}