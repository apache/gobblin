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

package gobblin.source.extractor.extract.kafka;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.metrics.kafka.SchemaRegistryException;


@Test(groups = { "gobblin.source.extractor.extract.kafka" })
public class SimpleKafkaSchemaRegistryTest {

  @Test
  public void testGetLatestSchemaByTopic() throws SchemaRegistryException {
    String topicName = "testTopicName";
    Assert.assertEquals(topicName, new SimpleKafkaSchemaRegistry(new Properties()).getLatestSchemaByTopic(topicName));
  }
}
