/*
 * Copyright (C) 2014-2015 NerdWallet All rights reserved.
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
import gobblin.metrics.kafka.SchemaNotFoundException;

import java.io.IOException;

import kafka.message.MessageAndOffset;


/**
 * An implementation of {@link KafkaExtractor} from which reads and returns records as an array of bytes.
 *
 * @author akshay@nerdwallet.com
 */
public class KafkaSimpleExtractor extends KafkaExtractor<String, byte[]> {

  public KafkaSimpleExtractor(WorkUnitState state) {
    super(state);
  }

  @Override
  protected byte[] decodeRecord(MessageAndOffset messageAndOffset) throws SchemaNotFoundException, IOException {
    return getBytes(messageAndOffset.message().payload());
  }

  /**
   * Get the schema (metadata) of the extracted data records.
   *
   * @return the Kafka topic being extracted
   * @throws IOException if there is problem getting the schema
   */
  @Override
  public String getSchema() throws IOException {
    return this.topicName;
  }
}
