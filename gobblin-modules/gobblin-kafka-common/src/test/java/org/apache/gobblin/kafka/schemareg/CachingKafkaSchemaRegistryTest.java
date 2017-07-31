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

package org.apache.gobblin.kafka.schemareg;

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



    CachingKafkaSchemaRegistry<Integer, String> cachingReg = new CachingKafkaSchemaRegistry<>(baseRegistry, 2);

    when(baseRegistry.register(name, schema1)).thenReturn(id1);
    Assert.assertEquals(cachingReg.register(name, schema1), id1);

    when(baseRegistry.register(name, schema2)).thenReturn(id2);
    Assert.assertEquals(cachingReg.register(name, schema2), id2);

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


  @Test
  public void testRegisterSchemaCaching()
      throws IOException, SchemaRegistryException {
    KafkaSchemaRegistry<Integer, String> baseRegistry = mock(KafkaSchemaRegistry.class);
    String name = "test";
    String schema1 = new String("schema");
    Integer id1 = 1;
    CachingKafkaSchemaRegistry<Integer, String> cachingReg = new CachingKafkaSchemaRegistry<>(baseRegistry, 2);

    when(baseRegistry.register(name, schema1)).thenReturn(id1);
    Assert.assertEquals(cachingReg.register(name, schema1), id1);


    Integer id2 = 2;
    when(baseRegistry.register(name, schema1)).thenReturn(id2);
    // Test that we get back the original id
    Assert.assertEquals(cachingReg.register(name, schema1), id1);
    // Ensure that we only called baseRegistry.register once.
    verify(baseRegistry, times(1)).register(anyString(), anyString());
  }


  @Test
  public void testIdSchemaCaching()
      throws IOException, SchemaRegistryException {
    KafkaSchemaRegistry<Integer, String> baseRegistry = mock(KafkaSchemaRegistry.class);
    String name = "test";
    String schema1 = new String("schema");
    Integer id1 = 1;
    CachingKafkaSchemaRegistry<Integer, String> cachingReg = new CachingKafkaSchemaRegistry<>(baseRegistry, 2);


    when(baseRegistry.getById(id1)).thenReturn(schema1);
    String schemaReturned = cachingReg.getById(id1);
    Assert.assertEquals(schemaReturned, schema1, "Schema returned by id should be the same");
    verify(baseRegistry, times(1)).getById(anyInt());

    when(baseRegistry.getById(id1)).thenReturn(new String("schema2"));
    Assert.assertEquals(cachingReg.getById(id1), schemaReturned);
    verify(baseRegistry, times(1)).getById(anyInt());

  }

  @Test
  public void testRegisterShouldCacheIds()
      throws IOException, SchemaRegistryException {

    KafkaSchemaRegistry<Integer, String> baseRegistry = mock(KafkaSchemaRegistry.class);
    CachingKafkaSchemaRegistry<Integer, String> cachingReg = new CachingKafkaSchemaRegistry<>(baseRegistry, 2);

    String name = "test";
    String schema1 = new String("schema");
    Integer id1 = 1;

    // first register name, schema1, get back id1
    when(baseRegistry.register(name, schema1)).thenReturn(id1);
    Assert.assertEquals(cachingReg.register(name, schema1), id1);

    // getById should hit the cache and return id1
    when(baseRegistry.getById(id1)).thenReturn(new String("schema2"));
    Assert.assertEquals(cachingReg.getById(id1), schema1);
    verify(baseRegistry, times(0)).getById(anyInt());
  }

}
