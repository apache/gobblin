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

package gobblin.converter.filter;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Closer;


/**
 * Tests for {@link AvroSchemaFieldRemover}
 */
@Test(groups = { "gobblin.converter.filter" })
public class AvroSchemaFieldRemoverTest {

  @Test
  public void testRemoveFields() throws IllegalArgumentException, IOException {
    Schema convertedSchema1 = convertSchema("/converter/recursive_schema_1.avsc", "YwchQiH.OjuzrLOtmqLW");
    Schema expectedSchema1 = parseSchema("/converter/recursive_schema_1_converted.avsc");
    Assert.assertEquals(convertedSchema1, expectedSchema1);

    Schema convertedSchema2 =
        convertSchema("/converter/recursive_schema_2.avsc", "FBuKC.wIINqII.lvaerUEKxBQUWg,eFQjDj.TzuYZajb");
    Schema expectedSchema2 = parseSchema("/converter/recursive_schema_2_converted.avsc");
    Assert.assertEquals(convertedSchema2, expectedSchema2);

    Schema convertedSchema3 = convertSchema("/converter/recursive_schema_2.avsc", "field.that.does.not.exist");
    Schema expectedSchema3 = parseSchema("/converter/recursive_schema_2_not_converted.avsc");
    Assert.assertEquals(convertedSchema3, expectedSchema3);
  }

  private Schema parseSchema(String schemaFile) throws IOException {
    Closer closer = Closer.create();
    try {
      InputStream in = closer.register(getClass().getResourceAsStream(schemaFile));
      return new Schema.Parser().parse(in);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  private Schema convertSchema(String schemaFile, String fieldsToRemove) throws IllegalArgumentException, IOException {
    Schema originalSchema = parseSchema(schemaFile);
    return new AvroSchemaFieldRemover(fieldsToRemove).removeFields(originalSchema);
  }
}
