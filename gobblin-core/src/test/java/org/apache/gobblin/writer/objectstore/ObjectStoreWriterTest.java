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
package org.apache.gobblin.writer.objectstore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.objectstore.ObjectStoreDeleteConverter;
import org.apache.gobblin.writer.objectstore.response.GetObjectResponse;


public class ObjectStoreWriterTest {

  private static final String SCHEMA_STR =
      "{ \"type\" : \"record\", \"name\" : \"test_schema\", \"namespace\" : \"com.gobblin.test\", "
          + "\"fields\" : [ { \"name\" : \"objectId\", \"type\" : \"string\"} ], \"doc:\" : \"\" }";

  @Test
  public void testDelete() throws Exception {

    WorkUnitState wu = new WorkUnitState();
    wu.setProp(ObjectStoreDeleteConverter.OBJECT_ID_FIELD, "objectId");

    ObjectStoreClient client = new MockObjectStoreClient();
    byte[] objId = client.put(IOUtils.toInputStream("test", "UTF-8"), ConfigFactory.empty());
    Assert.assertEquals(IOUtils.toString(client.getObject(objId).getObjectData(), "UTF-8"), "test");

    try (ObjectStoreWriter writer = new ObjectStoreWriter(client, new State());) {
      ObjectStoreDeleteConverter converter = new ObjectStoreDeleteConverter();
      converter.init(wu);

      Schema schema = new Schema.Parser().parse(SCHEMA_STR);
      GenericRecord datum = new GenericData.Record(schema);
      datum.put("objectId", objId);

      Iterables.getFirst(converter.convertRecord(converter.convertSchema(schema, wu), datum, wu), null);
      writer.write(Iterables.getFirst(
          converter.convertRecord(converter.convertSchema(schema, wu), datum, new WorkUnitState()), null));
    }

    try {
      client.getObject(objId);
      Assert.fail("should have thrown an IOException as object is already deleted");
    } catch (IOException e) {
      // All good exception thrown because object does not exist
    }
  }

  private static class MockObjectStoreClient implements ObjectStoreClient {

    private Map<byte[], String> store;

    public MockObjectStoreClient() {
      this.store = Maps.newHashMap();
    }

    @Override
    public byte[] put(InputStream objectStream, Config putConfig) throws IOException {
      byte[] objectId = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
      this.store.put(objectId, IOUtils.toString(objectStream, "UTF-8"));
      return objectId;
    }

    @Override
    public void delete(byte[] objectId, Config deletetConfig) throws IOException {
      this.store.remove(objectId);
    }

    @Override
    public Config getObjectProps(byte[] objectId) {
      return ConfigFactory.empty();
    }

    @Override
    public void close() throws IOException {
      this.store.clear();
    }

    @Override
    public GetObjectResponse getObject(byte[] objectId) throws IOException {
      if (!this.store.containsKey(objectId)) {
        throw new IOException("Object not found " + objectId);
      }
      return new GetObjectResponse(IOUtils.toInputStream(this.store.get(objectId), "UTF-8"), ConfigFactory.empty());
    }

    @Override
    public void setObjectProps(byte[] objectId, Config conf) throws IOException {}

    @Override
    public byte[] put(InputStream objectStream, byte[] objectId, Config putConfig) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
