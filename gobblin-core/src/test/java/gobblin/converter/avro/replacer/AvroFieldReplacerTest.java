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

package gobblin.converter.avro.replacer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.WorkUnitState;


public class AvroFieldReplacerTest {

  public static final Schema SCHEMA = (new Schema.Parser()).parse("{\"namespace\": \"example.avro\",\n"
      + " \"type\": \"record\",\n" + " \"name\": \"MyRecord\",\n" + " \"fields\": [\n"
      + "     {\"name\": \"name\", \"type\": \"string\"},\n"
      + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" + " ]\n" + "}");

  @Test
  public void test() throws Exception {

    GenericRecord record = new GenericRecordBuilder(SCHEMA).set("name", "myName").set("favorite_color", "blue").build();

    AvroFieldReplacer replacer = new AvroFieldReplacer();
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(AvroFieldReplacer.REPLACE_FIELD_KEY + ".name", "constant:myReplacement");

    Schema outputSchema = replacer.convertSchema(SCHEMA, workUnitState);
    Iterable<GenericRecord> it = replacer.convertRecord(outputSchema, record, workUnitState);
    GenericRecord outputRecord = it.iterator().next();
    Assert.assertEquals(outputRecord.get("name"), "myReplacement");
    Assert.assertEquals(outputRecord.get("favorite_color"), "blue");

    workUnitState.setProp(AvroFieldReplacer.REPLACE_FIELD_KEY + ".favorite_color", "constant:red");
    outputSchema = replacer.convertSchema(SCHEMA, workUnitState);
    it = replacer.convertRecord(outputSchema, record, workUnitState);
    outputRecord = it.iterator().next();
    Assert.assertEquals(outputRecord.get("name"), "myReplacement");
    Assert.assertEquals(outputRecord.get("favorite_color"), "red");

  }

}
