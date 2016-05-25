/*
 * Copyright (C) 2014-2016 NerdWallet All rights reserved.
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

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;

import java.io.IOException;

/**
 * A {@link KafkaSource} implementation for SimpleKafkaExtractor.
 *
 * @author akshay@nerdwallet.com
 *
 * @deprecated use {@link KafkaDeserializerSource} and {@link KafkaDeserializerExtractor.Deserializers#BYTE_ARRAY} instead
 */
public class KafkaSimpleSource extends KafkaSource<String, byte[]> {
  /**
   * Get an {@link Extractor} based on a given {@link WorkUnitState}.
   * <p>
   * The {@link Extractor} returned can use {@link WorkUnitState} to store arbitrary key-value pairs
   * that will be persisted to the state store and loaded in the next scheduled job run.
   * </p>
   *
   * @param state a {@link WorkUnitState} carrying properties needed by the returned {@link Extractor}
   * @return an {@link Extractor} used to extract schema and data records from the data source
   * @throws IOException if it fails to create an {@link Extractor}
   */
  @Override
  public Extractor<String, byte[]> getExtractor(WorkUnitState state) throws IOException {
    return new KafkaSimpleExtractor(state);
  }
}
