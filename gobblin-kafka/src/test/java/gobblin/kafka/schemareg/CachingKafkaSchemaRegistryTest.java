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

package gobblin.kafka.schemareg;

import java.io.IOException;

import static org.mockito.Mockito.*;

import org.testng.Assert;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class CachingKafkaSchemaRegistryTest {

  @Test
  public void testMaxReferences()
      throws IOException, SchemaRegistryException {
    KafkaSchemaRegistry<Integer, String> baseRegistry = mock(KafkaSchemaRegistry.class);
    String name = "test";
    String schema1 = new String("schema");
    String schema2 = new String("schema");
    String schema3 = new String("schema");
    Integer id1 = 1;
    Integer id2 = 2;
    Integer id3 = 3;


    when(baseRegistry.register(name, schema1)).thenReturn(id1);

    CachingKafkaSchemaRegistry<Integer, String> cachingReg = new CachingKafkaSchemaRegistry<>(baseRegistry, 2);

    Assert.assertEquals(cachingReg.register(name, schema1), id1);
    Assert.assertEquals(cachingReg.register(name, schema2), id2);
    Assert.assertEquals(cachingReg.register(name, schema2), id2);
    when(baseRegistry.register(name, schema2)).thenReturn(id2);
    when(baseRegistry.register(name, schema3)).thenReturn(id3);
    try {
      cachingReg.register(name, schema3);
      Assert.fail("Should have thrown an exception");
    }
    catch (Exception e)
    {
      log.info(e.getMessage());
    }

  }
}
