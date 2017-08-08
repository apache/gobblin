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
package org.apache.gobblin.converter.objectstore;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import org.apache.gobblin.configuration.WorkUnitState;


public class ObjectStoreDeleteConverterTest {

  @Test
  public void convertStringObjId() throws Exception {

    WorkUnitState wu = new WorkUnitState();
    wu.setProp(ObjectStoreDeleteConverter.OBJECT_ID_FIELD, "objectId");
    ObjectStoreDeleteConverter converter = new ObjectStoreDeleteConverter();
    converter.init(wu);
    Schema schema =
        new Schema.Parser()
            .parse("{ \"type\" : \"record\", \"name\" : \"test_schema\", \"namespace\" : \"com.gobblin.test\", "
                + "\"fields\" : [ { \"name\" : \"objectId\", \"type\" : \"string\"} ], \"doc:\" : \"\" }");
    GenericRecord datum = new GenericData.Record(schema);
    String objId = "abcd";
    datum.put("objectId", objId);

    Assert
        .assertEquals(Iterables.getFirst(converter.convertRecord(converter.convertSchema(schema, wu), datum, wu), null)
            .getObjectId(), objId.getBytes());

  }

  @Test
  public void convertLongObjId() throws Exception {
    WorkUnitState wu = new WorkUnitState();
    wu.setProp(ObjectStoreDeleteConverter.OBJECT_ID_FIELD, "objectId");
    ObjectStoreDeleteConverter converter = new ObjectStoreDeleteConverter();
    converter.init(wu);
    Schema schema =
        new Schema.Parser()
            .parse("{ \"type\" : \"record\", \"name\" : \"test_schema\", \"namespace\" : \"com.gobblin.test\", "
                + "\"fields\" : [ { \"name\" : \"objectId\", \"type\" : \"long\"} ], \"doc:\" : \"\" }");
    GenericRecord datum = new GenericData.Record(schema);
    long objId = 1234l;
    datum.put("objectId", objId);

    Assert
        .assertEquals(Iterables.getFirst(converter.convertRecord(converter.convertSchema(schema, wu), datum, wu), null)
            .getObjectId(), Longs.toByteArray(objId));
  }

  @Test
  public void convertBytesObjId() throws Exception {
    WorkUnitState wu = new WorkUnitState();
    wu.setProp(ObjectStoreDeleteConverter.OBJECT_ID_FIELD, "objectId");
    ObjectStoreDeleteConverter converter = new ObjectStoreDeleteConverter();
    converter.init(wu);
    Schema schema =
        new Schema.Parser()
            .parse("{ \"type\" : \"record\", \"name\" : \"test_schema\", \"namespace\" : \"com.gobblin.test\", "
                + "\"fields\" : [ { \"name\" : \"objectId\", \"type\" : \"bytes\"} ], \"doc:\" : \"\" }");
    GenericRecord datum = new GenericData.Record(schema);
    String objId = "abcd";
    datum.put("objectId", objId.getBytes());

    Assert
        .assertEquals(Iterables.getFirst(converter.convertRecord(converter.convertSchema(schema, wu), datum, wu), null)
            .getObjectId(), objId.getBytes());
  }

  @Test
  public void convertIntObjId() throws Exception {
    WorkUnitState wu = new WorkUnitState();
    wu.setProp(ObjectStoreDeleteConverter.OBJECT_ID_FIELD, "objectId");
    ObjectStoreDeleteConverter converter = new ObjectStoreDeleteConverter();
    converter.init(wu);
    Schema schema =
        new Schema.Parser()
            .parse("{ \"type\" : \"record\", \"name\" : \"test_schema\", \"namespace\" : \"com.gobblin.test\", "
                + "\"fields\" : [ { \"name\" : \"objectId\", \"type\" : \"int\"} ], \"doc:\" : \"\" }");
    GenericRecord datum = new GenericData.Record(schema);
    int objId = 123;
    datum.put("objectId", objId);

    Assert
        .assertEquals(Iterables.getFirst(converter.convertRecord(converter.convertSchema(schema, wu), datum, wu), null)
            .getObjectId(), Ints.toByteArray(objId));
  }
}
