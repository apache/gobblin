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

package gobblin.kafka.writer;

import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;


/**
 * Tests that don't need Kafka server to be running
 * */

@Slf4j
public class Kafka08DataWriterUnitTest {

  @Test
  public void testMinimalConfig()
  {
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, "FakeTopic");
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "bootstrap.servers", "localhost:9092");

    try {
      Kafka08DataWriter<GenericRecord> kafkaWriter = new Kafka08DataWriter<>(props);
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw exception", e);
    }
  }
}
