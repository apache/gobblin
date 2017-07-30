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
package gobblin.converter.avro;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;


public class BytesToAvroConverterTest {
  private static final String RESOURCE_PATH_PREFIX = "converter/bytes_to_avro/";

  @Test
  public void testCanParseBinary() throws DataConversionException, SchemaConversionException, IOException {
    InputStream schemaIn = getClass().getClassLoader().getResourceAsStream(RESOURCE_PATH_PREFIX + "test_record_schema.avsc");
    InputStream recordIn = getClass().getClassLoader().getResourceAsStream(RESOURCE_PATH_PREFIX + "test_record_binary.avro");
    Assert.assertNotNull("Could not load test schema from resources", schemaIn);
    Assert.assertNotNull("Could not load test record from resources", recordIn);

    BytesToAvroConverter converter = new BytesToAvroConverter();
    WorkUnitState state = new WorkUnitState();

    converter.init(state);
    Schema schema = converter.convertSchema(IOUtils.toString(schemaIn, StandardCharsets.UTF_8), state);

    Assert.assertEquals(schema.getName(), "testRecord");

    Iterator<GenericRecord> records = converter.convertRecord(schema, IOUtils.toByteArray(recordIn), state).iterator();
    GenericRecord record = records.next();

    Assert.assertFalse("Expected only 1 record", records.hasNext());

    Assert.assertEquals(record.get("testStr").toString(), "testing123");
    Assert.assertEquals(record.get("testInt"), -2);
  }
}
